# =========================================================
# TOKOPEDIA IMAGE RETRY ONLY - GABUNGAN_DATASET.csv
# ---------------------------------------------------------
# Tujuan:
# 1) Menghitung berapa banyak gambar produk yang belum berhasil terambil.
# 2) Mengambil ulang HANYA gambar yang gagal/kosong.
# 3) Menyimpan CSV baru, tanpa menimpa dataset lama.
# 4) Menyimpan report retry agar bisa dicek mana yang sukses/gagal.
#
# Persiapan pertama kali:
#   pip install pandas requests playwright
#   python -m playwright install chromium
#
# Cara jalan:
#   python retry_missing_product_images.py
# =========================================================

import os
import re
import csv
import json
import time
import random
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception


# =========================================================
# KONFIGURASI - UBAH BAGIAN INI SAJA
# =========================================================

INPUT_CSV = r"D:\Kuliah\DATA_TA\output2\GABUNGAN_DATASET_retry_images_clean.csv"
OUTPUT_CSV = r"output2/GABUNGAN_DATASET_retry_images_V2.csv"
TEMP_OUTPUT_CSV = r"output2/GABUNGAN_DATASET_retry_images_V2.tmp.csv"

REPORT_CSV = r"output2/retry_missing_images_report_round4.csv"
CHECKPOINT_PATH = r"output2/retry_missing_images_state_round4.json"
IMAGE_DIR = r"output2/retry_images_round4"

# Untuk cek jumlah gambar gagal saja tanpa download, ubah menjadi True.
DRY_RUN_COUNT_ONLY = False

# Untuk percobaan awal pakai 20/50 dulu. Jika sudah aman, naikkan 100, 300, dst.
# Isi None kalau ingin memproses semua gambar yang gagal.
# Jumlah produk missing yang diproses per batch.
# Karena AUTO_RUN aktif, LIMIT adalah per batch, bukan total keseluruhan.
LIMIT = 150

SAVE_EVERY = 25
HEADLESS = False

# Mode fallback:
# - False: coba download dari mediaURL_image_fixed/mediaURL_image di CSV saja. Lebih cepat.
# - True : jika URL gambar di CSV gagal, buka halaman produk dengan Playwright lalu ambil kandidat gambar dari DOM/og:image.
USE_PLAYWRIGHT_FALLBACK = True

PAGE_TIMEOUT_MS = 70000
WAIT_AFTER_LOAD_RANGE = (3.0, 5.0)

MIN_DELAY_BETWEEN_PRODUCTS = 2.5
MAX_DELAY_BETWEEN_PRODUCTS = 5.0
LONG_BREAK_EVERY = 100
LONG_BREAK_RANGE = (90.0, 180.0)

MAX_RETRIES_PER_IMAGE_URL = 1
MIN_IMAGE_BYTES = 15_000

# Auto-run beberapa batch agar bisa ditinggal.
AUTO_RUN = True
MAX_BATCHES = 5
SLEEP_BETWEEN_BATCHES = (30.0, 90.0)

# Produk/link yang sudah terbukti tidak tersedia tidak akan ikut retry lagi.
SKIP_UNAVAILABLE_PRODUCTS = True
UNAVAILABLE_NOTES = {
    "product_page_not_available",
    "product_page_unavailable_http",
    "product_page_redirected_away",
}

# Jangan aktifkan kecuali file lokal di image_local_path memang masih ada di laptop yang sama.
# Jika dataset dipindah folder/laptop, path lama biasanya tidak valid sehingga pengecekan file bisa menyesatkan.
CHECK_LOCAL_FILE_EXISTS = False

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)


# =========================================================
# HELPER UMUM
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


def id_to_str(x: Any) -> str:
    """Agar id dan shop_id tidak rusak ketika dibaca pandas/Excel."""
    s = safe_str(x)
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    if s.startswith("'"):
        s = s[1:].strip()
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    if re.fullmatch(r"\d+", s):
        return s
    ss = s.replace(",", ".")
    if re.search(r"[eE]", ss):
        try:
            d = Decimal(ss)
            return format(d.quantize(Decimal(1)), "f")
        except (InvalidOperation, ValueError):
            return s
    return s


def normalize_product_url(url: Any) -> str:
    u = safe_str(url)
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if not u.startswith("http"):
        return "https://" + u.lstrip("/")
    return u


def fix_img_url(u: Any) -> str:
    u = safe_str(u).replace("\\u0026", "&")
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    return u


def safe_file_stem(x: Any, fallback: str) -> str:
    s = id_to_str(x) or safe_str(x)
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", s)[:90]
    return s or fallback


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def load_checkpoint(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_old_row_indices": []}


def save_checkpoint(path: str, processed_indices: List[int]) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"processed_old_row_indices": sorted(set(map(int, processed_indices)))},
            f,
            ensure_ascii=False,
            indent=2,
        )


# =========================================================
# IDENTIFIKASI GAMBAR GAGAL
# =========================================================

def is_success_status(x: Any) -> bool:
    s = safe_str(x).lower()
    return s in {"200", "200.0", "ok", "success", "downloaded", "true"}


def is_blank_value(x: Any) -> bool:
    s = safe_str(x).lower()
    return s in {"", "nan", "none", "null", "-"}


def row_has_missing_image(row: pd.Series, check_local_file_exists: bool = False) -> bool:
    """Definisi gambar belum terambil.

    Dari struktur dataset Anda, gambar dianggap belum berhasil jika:
    - image_status kosong atau bukan 200, misalnya 403;
    - ATAU image_source kosong;
    - ATAU image_local_path kosong;
    - ATAU opsional: file pada image_local_path tidak ditemukan.
    """
    if SKIP_UNAVAILABLE_PRODUCTS:
        page_note = safe_str(row.get("product_page_note", ""))
        if page_note in UNAVAILABLE_NOTES:
            return False

    image_status = row.get("image_status", "")
    image_source = row.get("image_source", "")
    image_local_path = row.get("image_local_path", "")

    if not is_success_status(image_status):
        return True
    if is_blank_value(image_source):
        return True
    if is_blank_value(image_local_path):
        return True

    if check_local_file_exists:
        path = safe_str(image_local_path)
        if path and not os.path.exists(path):
            return True

    return False


def add_missing_image_flag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    needed_cols = ["image_status", "image_source", "image_local_path"]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = ""

    df["missing_image_before_retry"] = df.apply(
        lambda r: row_has_missing_image(r, CHECK_LOCAL_FILE_EXISTS), axis=1
    )
    return df


def print_missing_summary(df: pd.DataFrame) -> None:
    total = len(df)
    missing = int(df["missing_image_before_retry"].sum())
    ok = total - missing

    print("\n================= RINGKASAN GAMBAR DATASET =================")
    print(f"Total baris dataset              : {total:,}")
    print(f"Gambar sudah berhasil/aman       : {ok:,}")
    print(f"Gambar belum berhasil terambil   : {missing:,}")
    print(f"Persentase gambar gagal/kosong   : {(missing / total * 100 if total else 0):.2f}%")

    print("\nBreakdown image_status:")
    status_counts = df["image_status"].fillna("").astype(str).replace("", "<kosong>").value_counts(dropna=False)
    print(status_counts.head(30).to_string())

    print("============================================================\n")


# =========================================================
# FILTER KANDIDAT GAMBAR
# =========================================================

IMG_BAD_KEYWORDS = [
    "assets-tokopedia-lite", "icon", "logo", "sprite", "favicon",
    "/prod/", "/assets/", "/static/",
    "official_store", "badge_os", "badge_pm", "shop-badge", "shop_badge",
    "avatar", "shop_icon",

    # Tambahan agar tidak mengambil gambar error / placeholder Tokopedia
    "assets-unify",
    "il-error",
    "error-not-found",
    "not-found",
    "empty-state",
    "unavailable",
    "placeholder",
]


def is_probably_product_image(url: str) -> bool:
    u = safe_str(url).lower()
    if not u:
        return False

    if any(bad in u for bad in IMG_BAD_KEYWORDS):
        return False

    allowed_hosts = [
        "tokopedia-static",
        "images.tokopedia",
        "p16-images",
        "p19-images",
    ]

    if not any(host in u for host in allowed_hosts):
        return False

    return any(ext in u for ext in [".jpg", ".jpeg", ".png", ".webp", ".image"])


def get_csv_image_candidates(row: pd.Series) -> List[Tuple[str, str]]:
    candidates: List[Tuple[str, str]] = []
    for col in ["mediaURL_image_fixed", "mediaURL_image", "image_retry_url"]:
        if col in row.index:
            u = fix_img_url(row.get(col))
            if u and is_probably_product_image(u):
                candidates.append((col, u))
    return candidates


def extract_product_image_candidates_from_page(page) -> List[str]:
    candidates: List[str] = []

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

    seen = set()
    out = []
    for u in candidates:
        u = fix_img_url(u)
        if u and u not in seen and is_probably_product_image(u):
            seen.add(u)
            out.append(u)
    return out


def check_product_page_status(page) -> Dict[str, Any]:
    result = {
        "product_page_available": False,
        "product_page_title": "",
        "product_page_final_url": "",
        "product_page_note": "",
    }

    try:
        title = safe_str(page.title())
        final_url = safe_str(page.url)

        body_text = ""
        try:
            body_text = page.locator("body").inner_text(timeout=5000).lower()
        except Exception:
            body_text = ""

        result["product_page_title"] = title
        result["product_page_final_url"] = final_url

        bad_signals = [
            "produk tidak ditemukan",
            "halaman tidak ditemukan",
            "barang tidak ditemukan",
            "produk sudah tidak tersedia",
            "produk tidak tersedia",
            "produk dihapus",
            "produk sudah habis",
            "toko sedang tutup",
            "toko tidak ditemukan",
            "seller tidak ditemukan",
            "oops",
            "404",
        ]

        if any(x in body_text for x in bad_signals):
            result["product_page_available"] = False
            result["product_page_note"] = "product_page_not_available"
            return result

        if "tokopedia.com" in final_url:
            result["product_page_available"] = True
            result["product_page_note"] = "product_page_available"
            return result

        result["product_page_note"] = "unknown_page_status"
        return result

    except Exception as e:
        result["product_page_note"] = f"page_check_error:{type(e).__name__}:{str(e)[:120]}"
        return result


# =========================================================
# DOWNLOAD GAMBAR
# =========================================================

def download_image(session: requests.Session, img_url: str, out_path: str, referer: str) -> Tuple[bool, int, int, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": referer or "https://www.tokopedia.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }

    last_status = 0
    last_size = 0
    last_error = ""

    for attempt in range(1, MAX_RETRIES_PER_IMAGE_URL + 1):
        try:
            time.sleep(random.uniform(0.5, 1.5))
            r = session.get(img_url, headers=headers, timeout=45)
            last_status = int(r.status_code)
            last_size = len(r.content or b"")
            ctype = r.headers.get("content-type", "")

            if last_status == 200 and ctype.startswith("image/") and last_size >= MIN_IMAGE_BYTES:
                ensure_parent_dir(out_path)
                with open(out_path, "wb") as f:
                    f.write(r.content)
                return True, last_status, last_size, "downloaded"

            last_error = f"bad_response status={last_status} content_type={ctype} bytes={last_size}"

        except requests.RequestException as e:
            last_status = -1
            last_size = 0
            last_error = f"request_error:{type(e).__name__}:{str(e)[:150]}"

        time.sleep(random.uniform(1.0, 3.0) * attempt)

    return False, last_status, last_size, last_error


def retry_one_row(
    session: requests.Session,
    row: pd.Series,
    old_row_index: int,
    page=None,
) -> Dict[str, Any]:
    product_id = safe_file_stem(row.get("id"), fallback=f"row_{old_row_index}")
    product_url = normalize_product_url(row.get("url"))
    out_path = os.path.join(IMAGE_DIR, f"{product_id}.jpg")

    report = {
        "old_row_index": old_row_index,
        "id": id_to_str(row.get("id")),
        "name": safe_str(row.get("name")),
        "url": product_url,
        "image_status_before": safe_str(row.get("image_status")),
        "image_source_before": safe_str(row.get("image_source")),
        "image_local_path_before": safe_str(row.get("image_local_path")),
        "retry_attempted": True,
        "retry_success": False,
        "retry_status": "",
        "retry_bytes": 0,
        "retry_source": "",
        "retry_url": "",
        "retry_local_path": "",
        "retry_note": "",

        "product_page_available": "",
        "product_page_title": "",
        "product_page_final_url": "",
        "product_page_note": "",
    }

    candidate_urls: List[Tuple[str, str]] = get_csv_image_candidates(row)

    # Coba dari URL gambar yang sudah ada di CSV dulu.
    seen = set()
    for source, img_url in candidate_urls:
        if img_url in seen:
            continue
        seen.add(img_url)
        ok, status, size, note = download_image(session, img_url, out_path, product_url)
        report.update({
            "retry_status": status,
            "retry_bytes": size,
            "retry_source": source,
            "retry_url": img_url,
            "retry_note": note,
        })
        if ok:
            report.update({"retry_success": True, "retry_local_path": out_path})
            return report

    # Fallback: buka halaman produk, ambil og:image / img DOM.
    if USE_PLAYWRIGHT_FALLBACK and page is not None and product_url:
        try:
            response = page.goto(product_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            time.sleep(random.uniform(*WAIT_AFTER_LOAD_RANGE))

            try:
                page.wait_for_selector(
                    'meta[property="og:image"], meta[name="og:image"], img[src*="tokopedia"], img[src*="p16-images"], img[src*="p19-images"]',
                    timeout=10000
                )
            except Exception:
                pass

            if response is not None and response.status in {404, 410}:
                report["product_page_available"] = False
                report["product_page_final_url"] = safe_str(page.url)
                report["product_page_note"] = "product_page_unavailable_http"
                report["retry_note"] = "product_page_unavailable_http"
                return report

            page_status = check_product_page_status(page)

            report["product_page_available"] = page_status["product_page_available"]
            report["product_page_title"] = page_status["product_page_title"]
            report["product_page_final_url"] = page_status["product_page_final_url"]
            report["product_page_note"] = page_status["product_page_note"]

            if not page_status["product_page_available"]:
                report["retry_note"] = page_status["product_page_note"]
                return report

            page_candidates = extract_product_image_candidates_from_page(page)
            
            for img_url in page_candidates:
                if img_url in seen:
                    continue
                seen.add(img_url)
                ok, status, size, note = download_image(session, img_url, out_path, page.url)
                report.update({
                    "retry_status": status,
                    "retry_bytes": size,
                    "retry_source": "product_page_dom_or_og_image",
                    "retry_url": img_url,
                    "retry_note": note,
                })
                if ok:
                    report.update({"retry_success": True, "retry_local_path": out_path})
                    return report
        except Exception as e:
            report["retry_note"] = f"page_fallback_error:{type(e).__name__}:{str(e)[:150]}"

    if not report["retry_note"]:
        report["retry_note"] = "failed_no_valid_candidate_or_all_failed"
    return report


# =========================================================
# UPDATE CSV DAN REPORT
# =========================================================

REPORT_COLUMNS = [
    "old_row_index", "id", "name", "url",
    "image_status_before", "image_source_before", "image_local_path_before",
    "retry_attempted", "retry_success", "retry_status", "retry_bytes",
    "retry_source", "retry_url", "retry_local_path", "retry_note",
    "product_page_available", "product_page_title", "product_page_final_url", "product_page_note",
]


def append_report(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_parent_dir(REPORT_CSV)
    write_header = not os.path.exists(REPORT_CSV)
    pd.DataFrame(rows, columns=REPORT_COLUMNS).to_csv(
        REPORT_CSV,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )


def apply_success_to_dataframe(df: pd.DataFrame, report_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = df.copy()
    for col in ["image_status", "image_source", "image_local_path", "mediaURL_image_fixed"]:
        if col not in df.columns:
            df[col] = ""

    # Kolom audit baru agar jelas mana yang hasil retry.
    for col in ["image_retry_success", "image_retry_status", "image_retry_bytes", "image_retry_source", "image_retry_url", "image_retry_local_path", "image_retry_note"]:
        if col not in df.columns:
            df[col] = ""

    # Kolom status halaman produk. Produk yang tidak tersedia bisa di-skip pada run berikutnya.
    for col in ["product_page_available", "product_page_title", "product_page_final_url", "product_page_note"]:
        if col not in df.columns:
            df[col] = ""

    for r in report_rows:
        idx = int(r["old_row_index"])
        df.at[idx, "image_retry_success"] = str(r["retry_success"])
        df.at[idx, "image_retry_status"] = str(r["retry_status"])
        df.at[idx, "image_retry_bytes"] = str(r["retry_bytes"])
        df.at[idx, "image_retry_source"] = safe_str(r["retry_source"])
        df.at[idx, "image_retry_url"] = safe_str(r["retry_url"])
        df.at[idx, "image_retry_local_path"] = safe_str(r["retry_local_path"])
        df.at[idx, "image_retry_note"] = safe_str(r["retry_note"])
        df.at[idx, "product_page_available"] = safe_str(r.get("product_page_available", ""))
        df.at[idx, "product_page_title"] = safe_str(r.get("product_page_title", ""))
        df.at[idx, "product_page_final_url"] = safe_str(r.get("product_page_final_url", ""))
        df.at[idx, "product_page_note"] = safe_str(r.get("product_page_note", ""))

        if bool(r["retry_success"]):
            df.at[idx, "image_status"] = "200"
            df.at[idx, "image_source"] = safe_str(r["retry_source"])
            df.at[idx, "image_local_path"] = safe_str(r["retry_local_path"])
            if safe_str(r["retry_url"]):
                df.at[idx, "mediaURL_image_fixed"] = safe_str(r["retry_url"])

    return df


def main() -> None:
    SAME_INPUT_OUTPUT = os.path.abspath(INPUT_CSV) == os.path.abspath(OUTPUT_CSV)

    if SAME_INPUT_OUTPUT:
        actual_output_csv = TEMP_OUTPUT_CSV
    else:
        actual_output_csv = OUTPUT_CSV

    df = pd.read_csv(INPUT_CSV, dtype=str, keep_default_na=False)

    # Cek duplicate ID sebelum retry
    if "id" in df.columns:
        df["id"] = df["id"].apply(id_to_str)

        duplicate_id = df["id"].duplicated().sum()

        print(f"Duplicate ID sebelum retry: {duplicate_id}")

        if duplicate_id > 0:
            raise ValueError(
                "Dataset input memiliki duplicate ID. Bersihkan dulu sebelum retry."
            )
        
    if "shop_id" in df.columns:
        df["shop_id"] = df["shop_id"].apply(id_to_str)

    df = add_missing_image_flag(df)
    print_missing_summary(df)

    missing_indices = df.index[df["missing_image_before_retry"]].tolist()

    if DRY_RUN_COUNT_ONLY:
        print("[DRY RUN] Hanya menghitung gambar gagal. Tidak ada download.")
        return {"missing_total": len(missing_indices), "todo_count": 0, "processed_count": 0, "success_count": 0, "failed_count": 0}

    ckpt = load_checkpoint(CHECKPOINT_PATH)
    processed_indices = set(map(int, ckpt.get("processed_old_row_indices", [])))
    todo_indices = [i for i in missing_indices if i not in processed_indices]
    if LIMIT is not None:
        todo_indices = todo_indices[: int(LIMIT)]

    print(f"[INFO] Missing image total        : {len(missing_indices):,}")
    print(f"[INFO] Sudah pernah diproses     : {len(processed_indices):,}")
    print(f"[INFO] Akan diproses run ini     : {len(todo_indices):,}")
    print(f"[INFO] Output gambar             : {IMAGE_DIR}")
    print(f"[INFO] Report retry              : {REPORT_CSV}")
    print(f"[INFO] Dataset hasil update      : {OUTPUT_CSV}\n")

    if not todo_indices:
        print("[DONE] Tidak ada produk missing yang perlu diproses pada round ini.")
        return {"missing_total": len(missing_indices), "todo_count": 0, "processed_count": 0, "success_count": 0, "failed_count": 0}

    os.makedirs(IMAGE_DIR, exist_ok=True)
    session = requests.Session()
    batch_reports: List[Dict[str, Any]] = []
    all_reports_this_run: List[Dict[str, Any]] = []

    def process_loop(page=None):
        nonlocal batch_reports, all_reports_this_run, processed_indices
        for run_no, old_idx in enumerate(todo_indices, start=1):
            row = df.iloc[old_idx]
            print(f"[RETRY] {run_no}/{len(todo_indices)} old_row_index={old_idx} id={id_to_str(row.get('id'))}")
            report = retry_one_row(session, row, old_idx, page=page)
            print(f"        success={report['retry_success']} status={report['retry_status']} source={report['retry_source']} note={report['retry_note']}")

            batch_reports.append(report)
            all_reports_this_run.append(report)
            processed_indices.add(int(old_idx))

            if run_no % SAVE_EVERY == 0:
                append_report(batch_reports)
                batch_reports = []
                save_checkpoint(CHECKPOINT_PATH, list(processed_indices))
                print(f"[SAVE] checkpoint + report tersimpan sampai run_no={run_no}")

            if LONG_BREAK_EVERY and run_no % LONG_BREAK_EVERY == 0:
                pause = random.uniform(*LONG_BREAK_RANGE)
                print(f"[PAUSE] long break {pause:.1f}s")
                time.sleep(pause)
            else:
                time.sleep(random.uniform(MIN_DELAY_BETWEEN_PRODUCTS, MAX_DELAY_BETWEEN_PRODUCTS))

    if USE_PLAYWRIGHT_FALLBACK:
        if sync_playwright is None:
            raise RuntimeError("Playwright belum terinstall. Jalankan: pip install playwright && python -m playwright install chromium")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1366, "height": 900},
                locale="id-ID",
            )
            page = context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)
            page.set_default_navigation_timeout(PAGE_TIMEOUT_MS)
            process_loop(page=page)
            context.close()
            browser.close()
    else:
        process_loop(page=None)

    if batch_reports:
        append_report(batch_reports)
        batch_reports = []
    save_checkpoint(CHECKPOINT_PATH, list(processed_indices))

    updated_df = apply_success_to_dataframe(
        df,
        all_reports_this_run
    )

    if len(updated_df) != len(df):
        raise ValueError(
            f"Jumlah baris berubah! Sebelum: {len(df)}, Sesudah: {len(updated_df)}"
        )

    ensure_parent_dir(actual_output_csv)
    updated_df.to_csv(actual_output_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)

    if SAME_INPUT_OUTPUT:
        os.replace(actual_output_csv, OUTPUT_CSV)

    success_count = sum(1 for r in all_reports_this_run if r.get("retry_success"))
    failed_count = len(all_reports_this_run) - success_count
    print("\n================= HASIL RUN INI =================")
    print(f"Dicoba ulang      : {len(all_reports_this_run):,}")
    print(f"Berhasil          : {success_count:,}")
    print(f"Masih gagal       : {failed_count:,}")
    print(f"Report            : {REPORT_CSV}")
    print(f"CSV hasil update  : {OUTPUT_CSV}")
    print("=================================================\n")

    return {
        "missing_total": len(missing_indices),
        "todo_count": len(todo_indices),
        "processed_count": len(all_reports_this_run),
        "success_count": success_count,
        "failed_count": failed_count,
    }


def run_auto_batches() -> None:
    total_processed = 0
    total_success = 0
    total_failed = 0

    for batch_no in range(1, MAX_BATCHES + 1):
        print(f"\n================= AUTO BATCH {batch_no}/{MAX_BATCHES} =================")
        result = main() or {}

        if int(result.get("processed_count", 0)) == 0:
            print(
                "\n[SELESAI] Semua produk missing sudah pernah dicoba."
            )
            break

        processed = int(result.get("processed_count", 0))
        total_processed += processed
        total_success += int(result.get("success_count", 0))
        total_failed += int(result.get("failed_count", 0))

        if processed == 0:
            print("[AUTO DONE] Tidak ada item lagi yang diproses.")
            break

        print(
            f"[AUTO SUMMARY] processed_total={total_processed:,} "
            f"success_total={total_success:,} failed_total={total_failed:,}"
        )

        if batch_no < MAX_BATCHES:
            pause = random.uniform(*SLEEP_BETWEEN_BATCHES)
            print(f"[AUTO PAUSE] jeda antar batch {pause:.1f}s")
            time.sleep(pause)

    print("\n================= AUTO RUN SELESAI =================")
    print(f"Total dicoba   : {total_processed:,}")
    print(f"Total berhasil : {total_success:,}")
    print(f"Total gagal    : {total_failed:,}")
    print("====================================================\n")


if __name__ == "__main__":
    if AUTO_RUN:
        run_auto_batches()
    else:
        main()
