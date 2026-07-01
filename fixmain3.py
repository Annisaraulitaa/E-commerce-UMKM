# =========================================================
# TOKOPEDIA PRODUCT PAGE ENRICHMENT - SHOP BADGE + IMAGE RETRY
# ---------------------------------------------------------
# Tujuan:
# - BUKAN scraping ulang dataset dari awal.
# - Membaca CSV produk lama yang sudah ada.
# - Membuka URL produk satu per satu memakai Playwright.
# - Mengambil badge toko dari DOM: img[alt="shop badge"].
# - Output disimpan sebagai FILE BARU TERPISAH dan hanya berisi baris yang sudah diproses.
# - Output hanya menyimpan key merge minimal: old_row_index, id, name, url, shop_id.
# - ID diperlakukan sebagai STRING agar tidak rusak karena angka besar.
# - Sekaligus mencoba ulang download gambar untuk produk yang image_status-nya gagal/kosong.
# - Script ini TIDAK membuat scoring / labeling UMKM. Labeling dilakukan di file preprocessing terpisah.
#
# Persiapan pertama kali:
#   pip install pandas requests playwright
#   python -m playwright install chromium
#
# Cara jalan:
#   python fixmain3.py
# =========================================================

import os
import re
import csv
import json
import time
import random
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception


# =========================================================
# KONFIGURASI RUNNING
# =========================================================
# Ubah bagian ini saja.
# Pastikan INPUT_CSV dan OUTPUT_CSV tidak sama.

INPUT_CSV = r"output/GABUNGAN_DATASET.csv"
OUTPUT_CSV = r"output2/shop_badge_enrichment.csv"
CHECKPOINT_PATH = r"output2/shop_badge_enrichment_state.json"
IMAGE_DIR = r"output2/retry_images_badge"

# Untuk percobaan awal gunakan 10 atau 20 dulu.
# Naikkan bertahap ke 50 / 100 kalau sudah aman.
LIMIT = 25

# Simpan progress setiap N produk yang sudah diproses.
SAVE_EVERY = 5

# Browser terlihat atau tidak. Untuk debug awal, pakai False.
HEADLESS = False

# Delay antar produk agar tidak terlalu agresif.
MIN_DELAY_BETWEEN_PRODUCTS = 4.0
MAX_DELAY_BETWEEN_PRODUCTS = 9.0
LONG_BREAK_EVERY = 25
LONG_BREAK_RANGE = (90.0, 240.0)

# Timeout membuka halaman produk.
PAGE_TIMEOUT_MS = 45000
WAIT_AFTER_LOAD_RANGE = (2.5, 5.0)

# Retry gambar.
RETRY_MISSING_IMAGES = True
RETRY_ALL_IMAGES = False
MIN_IMAGE_BYTES = 20_000

# Kalau True, tambah kolom id_excel_safe dan shop_id_excel_safe agar aman dilihat di Excel.
# Untuk merge di pandas, gunakan kolom id dan shop_id yang normal, bukan id_excel_safe.
ADD_EXCEL_SAFE_ID_COLUMNS = True

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)


# =========================================================
# ID HELPERS
# =========================================================

def id_to_str(x: Any) -> str:
    """Normalisasi ID menjadi string.

    Catatan: jika file CSV sudah terlanjur menyimpan ID dalam scientific notation
    dengan presisi terpotong, misalnya 7.5E+18, nilai asli tidak bisa dipulihkan 100%.
    Solusi terbaik adalah membaca dari CSV asli yang masih menyimpan ID lengkap.
    """
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""

    # Hapus apostrophe Excel jika ada.
    if s.startswith("'"):
        s = s[1:].strip()

    # Jika sudah angka biasa, cukup bersihkan .0 di akhir.
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    if re.fullmatch(r"\d+", s):
        return s

    # Handle scientific notation, termasuk format koma Indonesia: 1,02E+11.
    ss = s.replace(",", ".")
    if re.search(r"[eE]", ss):
        try:
            d = Decimal(ss)
            return format(d.quantize(Decimal(1)), "f")
        except (InvalidOperation, ValueError):
            pass

    # Fallback: ambil digit saja kalau formatnya kotor.
    digits = re.sub(r"[^0-9]", "", s)
    return digits if digits else s


def excel_safe_text(x: Any) -> str:
    s = id_to_str(x)
    return "'" + s if s else ""


def normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["id", "shop_id"]:
        if col in df.columns:
            df[col] = df[col].apply(id_to_str)
        else:
            df[col] = ""
    if "url" not in df.columns:
        df["url"] = ""
    if "name" not in df.columns:
        df["name"] = ""
    return df


# =========================================================
# GENERAL HELPERS
# =========================================================

def safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_json(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return ""


def normalize_product_url(url: Any) -> str:
    u = safe_str(url)
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    elif not u.startswith("http"):
        u = "https://" + u.lstrip("/")
    return u


def safe_file_stem(x: Any, fallback: str) -> str:
    s = id_to_str(x) or safe_str(x)
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", s)[:90]
    return s or fallback


def load_checkpoint(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_index": -1, "failed_indices": []}


def save_checkpoint(path: str, last_index: int, failed_indices: List[int]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"last_index": int(last_index), "failed_indices": list(sorted(set(failed_indices)))},
            f,
            ensure_ascii=False,
            indent=2,
        )


# =========================================================
# SHOP BADGE EXTRACTION
# =========================================================

def infer_shop_badge_type(src: str, alt: str = "") -> str:
    """Infer jenis badge dari URL image / alt.

    Mapping ini dibuat konservatif. Jika Tokopedia mengganti nama file/path,
    hasil bisa UNKNOWN_BADGE walaupun badge terlihat di halaman.
    """
    s = f"{src or ''} {alt or ''}".lower()

    # Official Store. Dari screenshot user terlihat path official_store/badge_os.png.
    if "official_store" in s or "badge_os" in s or "official-store" in s:
        return "OFFICIAL_STORE"

    # Power Merchant Pro / Power Merchant. Pattern dibuat luas karena nama asset bisa berbeda.
    if "power_merchant_pro" in s or "power-merchant-pro" in s or "badge_pmp" in s or "pm_pro" in s:
        return "POWER_MERCHANT_PRO"
    if "power_merchant" in s or "power-merchant" in s or "badge_pm" in s:
        return "POWER_MERCHANT"

    # Mall / official mall jika muncul.
    if "tokopedia_mall" in s or "badge_mall" in s or "/mall/" in s:
        return "MALL"

    if src:
        return "UNKNOWN_BADGE"
    return "NO_BADGE"


def extract_badges_from_page(page) -> Dict[str, Any]:
    """Ambil semua img[alt='shop badge'] dari halaman produk."""
    badges = page.evaluate(
        """
        () => Array.from(document.querySelectorAll('img[alt="shop badge"]')).map(img => ({
            src: img.getAttribute('src') || '',
            alt: img.getAttribute('alt') || '',
            width: img.naturalWidth || img.width || null,
            height: img.naturalHeight || img.height || null
        }))
        """
    )
    badges = badges or []

    # Deduplicate src.
    seen = set()
    unique = []
    for b in badges:
        src = safe_str((b or {}).get("src"))
        if src and src not in seen:
            seen.add(src)
            unique.append(b)

    badge_types = [infer_shop_badge_type(b.get("src", ""), b.get("alt", "")) for b in unique]
    primary_type = "NO_BADGE"
    priority = ["OFFICIAL_STORE", "MALL", "POWER_MERCHANT_PRO", "POWER_MERCHANT", "UNKNOWN_BADGE"]
    for t in priority:
        if t in badge_types:
            primary_type = t
            break

    return {
        "shop_badge_found": bool(unique),
        "shop_badge_count": len(unique),
        "shop_badge_type": primary_type,
        "shop_badge_types_json": safe_json(badge_types),
        "shop_badge_srcs_json": safe_json([b.get("src", "") for b in unique]),
        "shop_badge_alts_json": safe_json([b.get("alt", "") for b in unique]),
        "shop_badge_primary_src": unique[0].get("src", "") if unique else "",
        "is_official_store_dom": primary_type == "OFFICIAL_STORE",
        "is_power_merchant_dom": primary_type == "POWER_MERCHANT",
        "is_power_merchant_pro_dom": primary_type == "POWER_MERCHANT_PRO",
        "is_mall_dom": primary_type == "MALL",
    }


# =========================================================
# IMAGE RETRY
# =========================================================

IMG_BAD_KEYWORDS = [
    "assets-tokopedia-lite", "icon", "logo", "sprite", "favicon", "/prod/", "/assets/", "/static/",
    "official_store", "badge_os", "badge_pm", "shop-badge", "shop_badge",
]


def fix_img_url(u: Any) -> str:
    if u is None:
        return ""
    try:
        if pd.isna(u):
            return ""
    except Exception:
        pass
    u = str(u).replace("\\u0026", "&").strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    return u


def is_probably_product_image(url: str) -> bool:
    u = safe_str(url).lower()
    if not u:
        return False
    if "tokopedia-static" not in u and "images.tokopedia" not in u:
        return False
    if any(bad in u for bad in IMG_BAD_KEYWORDS):
        return False
    return any(ext in u for ext in [".jpg", ".jpeg", ".png", ".webp"])


def should_retry_image(row: pd.Series, retry_all_images: bool = False) -> bool:
    if retry_all_images:
        return True
    status = safe_str(row.get("image_status"))
    source = safe_str(row.get("image_source"))
    path = safe_str(row.get("image_local_path"))

    if not status or status.lower() in {"nan", "none"}:
        return True
    if status not in {"200", "200.0"}:
        return True
    if not source or source.lower() in {"nan", "none"}:
        return True
    if not path or path.lower() in {"nan", "none"}:
        return True
    return False


def extract_product_image_candidates_from_page(page) -> List[str]:
    candidates = []

    # 1) og:image biasanya paling bersih.
    try:
        og_images = page.evaluate(
            """
            () => Array.from(document.querySelectorAll('meta[property="og:image"], meta[name="og:image"]'))
                .map(m => m.getAttribute('content') || '')
                .filter(Boolean)
            """
        ) or []
        candidates.extend(og_images)
    except Exception:
        pass

    # 2) semua image kandidat produk.
    try:
        dom_images = page.evaluate(
            """
            () => Array.from(document.querySelectorAll('img')).map(img => ({
                src: img.currentSrc || img.src || img.getAttribute('src') || '',
                alt: img.getAttribute('alt') || '',
                width: img.naturalWidth || img.width || 0,
                height: img.naturalHeight || img.height || 0
            }))
            .filter(x => x.src)
            .sort((a,b) => (b.width*b.height) - (a.width*a.height))
            .map(x => x.src)
            """
        ) or []
        candidates.extend(dom_images)
    except Exception:
        pass

    seen, out = set(), []
    for u in candidates:
        u = fix_img_url(u)
        if u and u not in seen and is_probably_product_image(u):
            seen.add(u)
            out.append(u)
    return out


def download_image(session: requests.Session, img_url: str, out_path: str, referer: str) -> Tuple[bool, int, int]:
    img_url = fix_img_url(img_url)
    if not img_url:
        return False, 0, 0

    headers = {
        "User-Agent": USER_AGENT,
        "Referer": referer or "https://www.tokopedia.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    try:
        time.sleep(random.uniform(0.5, 1.2))
        r = session.get(img_url, headers=headers, timeout=45)
        status = r.status_code
        size = len(r.content or b"")
        ctype = r.headers.get("content-type", "")
        if status == 200 and ctype.startswith("image/") and size >= MIN_IMAGE_BYTES:
            os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(r.content)
            return True, status, size
        return False, status, size
    except requests.RequestException:
        return False, -1, 0


def retry_product_image(
    session: requests.Session,
    row: pd.Series,
    page,
    product_url: str,
    image_dir: str,
    retry_all_images: bool = False,
) -> Dict[str, Any]:
    result = {
        "image_retry_attempted": False,
        "image_retry_success": False,
        "image_retry_status": None,
        "image_retry_bytes": None,
        "image_retry_source": None,
        "image_retry_url": None,
        "image_retry_local_path": None,
        "image_retry_note": None,
    }

    if not should_retry_image(row, retry_all_images=retry_all_images):
        result["image_retry_note"] = "skip_existing_ok"
        return result

    result["image_retry_attempted"] = True
    product_id = safe_file_stem(row.get("id"), fallback=f"row_{row.name}")
    out_path = os.path.join(image_dir, f"{product_id}.jpg")

    candidate_urls: List[Tuple[str, str]] = []

    # URL gambar lama dari CSV.
    for col in ["mediaURL_image_fixed", "mediaURL_image"]:
        u = fix_img_url(row.get(col))
        if u:
            candidate_urls.append((col, u))

    # Kandidat dari halaman produk.
    try:
        for u in extract_product_image_candidates_from_page(page):
            candidate_urls.append(("product_page_dom", u))
    except Exception:
        pass

    seen = set()
    for source, img_url in candidate_urls:
        if not img_url or img_url in seen:
            continue
        seen.add(img_url)
        ok, status, size = download_image(session, img_url, out_path, referer=product_url)
        result.update({
            "image_retry_status": status,
            "image_retry_bytes": size,
            "image_retry_source": source,
            "image_retry_url": img_url,
        })
        if ok:
            result.update({
                "image_retry_success": True,
                "image_retry_local_path": out_path,
                "image_retry_note": "downloaded",
            })
            return result

    result["image_retry_note"] = "failed"
    return result


# =========================================================
# OUTPUT
# =========================================================

OUTPUT_COLUMNS = [
    "old_row_index",
    "id", "id_excel_safe",
    "name", "url",
    "shop_id", "shop_id_excel_safe",
    "page_status", "page_http_status", "page_final_url", "page_error",
    "shop_badge_found", "shop_badge_count", "shop_badge_type",
    "shop_badge_primary_src", "shop_badge_srcs_json", "shop_badge_types_json", "shop_badge_alts_json",
    "is_official_store_dom", "is_power_merchant_dom", "is_power_merchant_pro_dom", "is_mall_dom",
    "image_retry_attempted", "image_retry_success", "image_retry_status", "image_retry_bytes",
    "image_retry_source", "image_retry_url", "image_retry_local_path", "image_retry_note",
]


def build_output_row(
    old_index: int,
    row: pd.Series,
    page_status: str,
    page_http_status: Optional[int] = None,
    page_final_url: str = "",
    page_error: str = "",
    badge_features: Optional[Dict[str, Any]] = None,
    image_features: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "old_row_index": old_index,
        "id": id_to_str(row.get("id")),
        "name": safe_str(row.get("name")),
        "url": normalize_product_url(row.get("url")),
        "shop_id": id_to_str(row.get("shop_id")),
        "page_status": page_status,
        "page_http_status": page_http_status,
        "page_final_url": page_final_url,
        "page_error": page_error,
    }

    if ADD_EXCEL_SAFE_ID_COLUMNS:
        out["id_excel_safe"] = excel_safe_text(row.get("id"))
        out["shop_id_excel_safe"] = excel_safe_text(row.get("shop_id"))

    if badge_features:
        out.update(badge_features)
    if image_features:
        out.update(image_features)

    # Pastikan semua kolom ada agar struktur CSV stabil.
    for col in OUTPUT_COLUMNS:
        out.setdefault(col, None)
    return {col: out.get(col) for col in OUTPUT_COLUMNS}


def append_rows(output_csv: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    write_header = not os.path.exists(output_csv)
    pd.DataFrame(rows, columns=OUTPUT_COLUMNS).to_csv(
        output_csv,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )


# =========================================================
# MAIN
# =========================================================

def enrich_shop_badges_from_product_pages(
    input_csv: str,
    output_csv: str,
    checkpoint_path: str,
    image_dir: str,
    limit: Optional[int] = None,
    save_every: int = 5,
    headless: bool = False,
    retry_missing_images: bool = True,
    retry_all_images: bool = False,
) -> None:
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright belum terinstall. Jalankan: pip install playwright && python -m playwright install chromium"
        )

    if os.path.abspath(input_csv) == os.path.abspath(output_csv):
        raise ValueError("INPUT_CSV dan OUTPUT_CSV tidak boleh sama agar dataset lama tidak tertimpa.")

    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    df = normalize_key_columns(df)

    ckpt = load_checkpoint(checkpoint_path)
    start = int(ckpt.get("last_index", -1)) + 1
    failed_indices = list(ckpt.get("failed_indices", []))
    end = len(df) if limit is None else min(len(df), start + int(limit))

    os.makedirs(image_dir, exist_ok=True)
    session = requests.Session()

    print(f"[START] product-page badge enrichment index {start} sampai {end - 1}")
    print(f"[INFO] output hanya append baris yang diproses -> {output_csv}")

    batch: List[Dict[str, Any]] = []
    processed_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 900},
            locale="id-ID",
        )
        page = context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)
        page.set_default_navigation_timeout(PAGE_TIMEOUT_MS)

        for i in range(start, end):
            row = df.iloc[i]
            product_url = normalize_product_url(row.get("url"))

            if not product_url:
                out = build_output_row(i, row, page_status="skip_no_url", page_error="url kosong")
                batch.append(out)
                failed_indices.append(i)
                save_checkpoint(checkpoint_path, i, failed_indices)
                continue

            badge_features: Dict[str, Any] = {}
            image_features: Dict[str, Any] = {}
            page_status = "unknown"
            page_http_status = None
            page_final_url = ""
            page_error = ""

            try:
                print(f"[OPEN] index={i} id={id_to_str(row.get('id'))} url={product_url[:90]}")
                response = page.goto(product_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
                page_http_status = response.status if response else None
                page_final_url = page.url
                time.sleep(random.uniform(*WAIT_AFTER_LOAD_RANGE))

                badge_features = extract_badges_from_page(page)
                page_status = "success"

                if retry_missing_images:
                    image_features = retry_product_image(
                        session=session,
                        row=row,
                        page=page,
                        product_url=product_url,
                        image_dir=image_dir,
                        retry_all_images=retry_all_images,
                    )

                print(
                    f"[OK] index={i} badge={badge_features.get('shop_badge_type')} "
                    f"img={image_features.get('image_retry_success') if image_features else 'skip'}"
                )

            except PlaywrightTimeoutError as e:
                page_status = "timeout"
                page_error = str(e)[:250]
                failed_indices.append(i)
                print(f"[TIMEOUT] index={i} {page_error}")

            except Exception as e:
                page_status = f"error:{type(e).__name__}"
                page_error = str(e)[:250]
                failed_indices.append(i)
                print(f"[ERROR] index={i} {type(e).__name__}: {page_error}")

            out = build_output_row(
                old_index=i,
                row=row,
                page_status=page_status,
                page_http_status=page_http_status,
                page_final_url=page_final_url,
                page_error=page_error,
                badge_features=badge_features,
                image_features=image_features,
            )
            batch.append(out)
            processed_count += 1

            if processed_count % save_every == 0:
                append_rows(output_csv, batch)
                batch.clear()
                save_checkpoint(checkpoint_path, i, failed_indices)
                print(f"[SAVE] progress index={i} -> {output_csv}")

            if LONG_BREAK_EVERY and processed_count % LONG_BREAK_EVERY == 0:
                pause = random.uniform(*LONG_BREAK_RANGE)
                print(f"[PAUSE] long break {pause:.1f}s after {processed_count} processed rows")
                time.sleep(pause)
            else:
                time.sleep(random.uniform(MIN_DELAY_BETWEEN_PRODUCTS, MAX_DELAY_BETWEEN_PRODUCTS))

        if batch:
            append_rows(output_csv, batch)
            batch.clear()

        context.close()
        browser.close()

    save_checkpoint(checkpoint_path, end - 1, failed_indices)
    print(f"[DONE] saved -> {output_csv}")


if __name__ == "__main__":
    enrich_shop_badges_from_product_pages(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        checkpoint_path=CHECKPOINT_PATH,
        image_dir=IMAGE_DIR,
        limit=LIMIT,
        save_every=SAVE_EVERY,
        headless=HEADLESS,
        retry_missing_images=RETRY_MISSING_IMAGES,
        retry_all_images=RETRY_ALL_IMAGES,
    )
