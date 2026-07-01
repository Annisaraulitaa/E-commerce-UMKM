# =========================================================
# TOKOPEDIA SHOPINFOCORE ENRICHMENT - SHOP LEVEL RAW DATA
# ---------------------------------------------------------
# Tujuan:
# - BUKAN scraping ulang produk dari awal.
# - Membaca dataset produk lama.
# - Mengambil toko unik berdasarkan shop_url / domain toko.
# - Request GraphQL ShopInfoCore per toko.
# - Menyimpan output mentah level toko ke CSV baru.
# - Tidak membuat label UMKM, tidak membuat skor, tidak mengubah file lama.
#
# Output bisa di-merge ke dataset produk berdasarkan:
# - shop_domain, atau
# - shop_id/shopinfo_shop_id jika format ID masih aman.
# =========================================================

import os
import re
import json
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests


# =========================================================
# 1) KONFIGURASI RUNNING
# =========================================================
# Ubah bagian ini sesuai lokasi file kamu.
# INPUT_CSV dan OUTPUT_CSV jangan sama.

INPUT_CSV = r"output/GABUNGAN_DATASET.csv"
OUTPUT_CSV = r"output3/shopinfo_core_enrichment.csv"
CHECKPOINT_PATH = r"output3/shopinfo_core_state.json"

# Untuk test awal, gunakan 20/50 dulu.
# Jika sudah aman, naikkan bertahap: 100, 300, dst.
LIMIT = 120

# Simpan progress setiap N toko.
SAVE_EVERY = 10

# Delay agar tidak terlalu agresif.
MIN_DELAY = 1.5
MAX_DELAY = 3.5
LONG_BREAK_EVERY = 60
LONG_BREAK_RANGE = (60.0, 150.0)
MAX_RETRIES = 4
TIMEOUT = 60

# Jika True, output akan punya kolom shop_id_excel_safe dengan awalan petik satu.
# Ini hanya untuk dibuka di Excel agar tidak berubah jadi 7.5E+18.
ADD_EXCEL_SAFE_ID_COLS = True


# =========================================================
# 2) SHOPINFOCORE GRAPHQL CONFIG
# =========================================================

BD_DEVICE_ID = "7527216031573837313"

SHOPINFO_CFG = {
    "url": "https://gql.tokopedia.com/graphql/ShopInfoCore",
    "headers": {
        "accept": "*/*",
        "bd-device-id": BD_DEVICE_ID,
        "content-type": "application/json",
        "referer": "https://www.tokopedia.com/",
        "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        "x-price-center": "true",
        "x-source": "tokopedia-lite",
        "x-tkpd-lite-service": "zeus",
        "x-version": "f2b5e35",
    },
    "operationName": "ShopInfoCore",
    "query": """query ShopInfoCore($id: Int!, $domain: String) {
  shopInfoByID(input: {shopIDs: [$id], fields: ["active_product", "allow_manage_all", "assets", "core", "closed_info", "create_info", "favorite", "location", "status", "is_open", "other-goldos", "shipment", "shopstats", "shop-snippet", "other-shiploc", "shopHomeType", "goapotik", "fs_type"], domain: $domain, source: "shoppage"}) {
    result {
      shopCore {
        domain
        shopID
        name
        defaultSort
        __typename
      }
      createInfo {
        openSince
        __typename
      }
      favoriteData {
        totalFavorite
        alreadyFavorited
        __typename
      }
      activeProduct
      shopAssets {
        avatar
        cover
        __typename
      }
      location
      isAllowManage
      isOpen
      shipmentInfo {
        isAvailable
        image
        name
        product {
          isAvailable
          productName
          uiHidden
          __typename
        }
        __typename
      }
      shippingLoc {
        districtName
        cityName
        __typename
      }
      shopStats {
        productSold
        totalTxSuccess
        totalShowcase
        __typename
      }
      statusInfo {
        shopStatus
        statusMessage
        statusTitle
        tickerType
        __typename
      }
      closedInfo {
        closedNote
        until
        reason
        detail {
          status
          __typename
        }
        __typename
      }
      bbInfo {
        bbName
        bbDesc
        bbNameEN
        bbDescEN
        __typename
      }
      goldOS {
        isGold
        isGoldBadge
        isOfficial
        badge
        shopTier
        __typename
      }
      shopSnippetURL
      customSEO {
        title
        description
        bottomContent
        __typename
      }
      isQA
      isGoApotik
      partnerInfo {
        fsType
        __typename
      }
      __typename
    }
    error {
      message
      __typename
    }
    __typename
  }
}
""",
}


# =========================================================
# 3) HELPERS
# =========================================================

BADGE_TYPE_PATTERNS = [
    ("OFFICIAL_STORE", ["official_store", "badge_os", "/os/", "official"]),
    ("POWER_MERCHANT_PRO", ["power_merchant_pro", "badge_pmp", "merchant_pro", "pmp"]),
    ("POWER_MERCHANT", ["power_merchant", "badge_pm", "gold", "pm_badge"]),
]


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
    """Ubah ID menjadi string tanpa scientific notation jika masih memungkinkan."""
    s = safe_str(x)
    if not s:
        return ""

    # Jika dibaca dari CSV sebagai float/scientific notation, coba normalisasi.
    # Catatan: kalau file sudah pernah disimpan ulang dari Excel dan presisi hilang,
    # tidak ada cara pasti untuk mengembalikan ID asli.
    try:
        if re.fullmatch(r"\d+", s):
            return s
        if re.search(r"[eE]", s) or re.fullmatch(r"\d+\.0", s):
            return str(int(float(s)))
    except Exception:
        pass

    return s


def excel_safe_id(x: Any) -> str:
    s = id_to_str(x)
    return ("'" + s) if s else ""


def safe_int(x: Any) -> Optional[int]:
    s = safe_str(x)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        digits = re.sub(r"[^0-9]", "", s)
        return int(digits) if digits else None


def safe_json_dumps(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return ""


def extract_shop_domain_from_url(url: Any) -> str:
    """Ambil domain toko dari shop_url atau product_url Tokopedia.

    Contoh:
    - https://www.tokopedia.com/white-inc -> white-inc
    - https://www.tokopedia.com/white-inc/produk-abc -> white-inc
    """
    s = safe_str(url)
    if not s:
        return ""
    if not s.startswith("http"):
        s = "https://" + s.lstrip("/")
    try:
        parsed = urlparse(s)
        parts = [p for p in parsed.path.split("/") if p]
        return parts[0].strip().lower() if parts else ""
    except Exception:
        return ""


def infer_badge_type(badge_url: Any, is_official: Any = None, is_gold: Any = None, is_gold_badge: Any = None) -> str:
    url = safe_str(badge_url).lower()
    official = safe_int(is_official) == 1
    gold = safe_int(is_gold) == 1
    gold_badge = safe_int(is_gold_badge) == 1

    if official:
        return "OFFICIAL_STORE"

    for badge_type, keywords in BADGE_TYPE_PATTERNS:
        if any(k in url for k in keywords):
            return badge_type

    if gold or gold_badge:
        return "GOLD_OR_POWER_MERCHANT"

    if url:
        return "UNKNOWN_BADGE"
    return "NO_BADGE"


def strip_html(raw: Any) -> str:
    s = safe_str(raw)
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# =========================================================
# 4) REQUESTER
# =========================================================

class PoliteRequester:
    def __init__(
        self,
        min_delay: float = MIN_DELAY,
        max_delay: float = MAX_DELAY,
        long_break_every: int = LONG_BREAK_EVERY,
        long_break_range: Tuple[float, float] = LONG_BREAK_RANGE,
        max_retries: int = MAX_RETRIES,
        backoff_base: float = 2.0,
        backoff_cap: float = 180.0,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.long_break_every = long_break_every
        self.long_break_range = long_break_range
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.req_count = 0

    def _sleep(self):
        time.sleep(random.uniform(self.min_delay, self.max_delay))
        self.req_count += 1
        if self.long_break_every and self.req_count % self.long_break_every == 0:
            pause = random.uniform(*self.long_break_range)
            print(f"[PAUSE] long break {pause:.1f}s after {self.req_count} requests")
            time.sleep(pause)

    def post_json(self, session: requests.Session, url: str, headers: Dict[str, str], payload: Any, timeout: int = TIMEOUT) -> Any:
        for attempt in range(self.max_retries + 1):
            self._sleep()
            try:
                r = session.post(url, headers=headers, json=payload, timeout=timeout)

                if r.status_code in (429, 403, 500, 502, 503, 504):
                    retry_after = r.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait = min(float(retry_after), self.backoff_cap)
                    else:
                        wait = min((self.backoff_base ** attempt) + random.uniform(5.0, 12.0), self.backoff_cap)
                    print(f"[BACKOFF] HTTP {r.status_code} attempt={attempt + 1}/{self.max_retries + 1} wait={wait:.1f}s")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                wait = min((self.backoff_base ** attempt) + random.uniform(5.0, 12.0), self.backoff_cap)
                print(f"[RETRY] {type(e).__name__} attempt={attempt + 1}/{self.max_retries + 1} wait={wait:.1f}s")
                time.sleep(wait)

        raise RuntimeError("Max retries exceeded")


# =========================================================
# 5) SHOPINFO FETCH + FLATTEN
# =========================================================

def fetch_shopinfo_core(pr: PoliteRequester, session: requests.Session, domain: str, shop_id: Optional[str] = None) -> Dict[str, Any]:
    """Request ShopInfoCore.

    Prioritas utama memakai domain, dengan id=0 karena payload browser menunjukkan ini valid.
    Jika domain kosong tetapi shop_id tersedia, coba memakai shop_id.
    """
    domain = safe_str(domain).lower()
    sid_int = 0

    if not domain and shop_id:
        maybe = safe_int(shop_id)
        sid_int = maybe if maybe is not None else 0

    payload = [{
        "operationName": SHOPINFO_CFG["operationName"],
        "variables": {
            "id": sid_int,
            "domain": domain or None,
        },
        "query": SHOPINFO_CFG["query"],
    }]

    resp = pr.post_json(
        session=session,
        url=SHOPINFO_CFG["url"],
        headers=SHOPINFO_CFG["headers"],
        payload=payload,
        timeout=TIMEOUT,
    )
    root = resp[0] if isinstance(resp, list) and resp else resp
    return root or {}


def flatten_shopinfo_response(root: Dict[str, Any]) -> Dict[str, Any]:
    base = {
        "shopinfo_status": "empty_response",
        "shopinfo_error_message": None,
        "shopinfo_raw_json": safe_json_dumps(root),
    }

    try:
        obj = root.get("data", {}).get("shopInfoByID", {})
        err = obj.get("error") or {}
        err_msg = err.get("message")
        result = obj.get("result") or []

        if err_msg:
            base["shopinfo_error_message"] = err_msg

        if not result:
            base["shopinfo_status"] = "not_found"
            return base

        info = result[0] or {}
        shop_core = info.get("shopCore") or {}
        create_info = info.get("createInfo") or {}
        favorite = info.get("favoriteData") or {}
        assets = info.get("shopAssets") or {}
        shipping_loc = info.get("shippingLoc") or {}
        stats = info.get("shopStats") or {}
        status_info = info.get("statusInfo") or {}
        closed = info.get("closedInfo") or {}
        closed_detail = (closed.get("detail") or {}) if isinstance(closed, dict) else {}
        gold_os = info.get("goldOS") or {}
        seo = info.get("customSEO") or {}
        partner = info.get("partnerInfo") or []
        bb_info = info.get("bbInfo") or []
        shipment_info = info.get("shipmentInfo") or []

        badge_url = gold_os.get("badge")
        is_official = gold_os.get("isOfficial")
        is_gold = gold_os.get("isGold")
        is_gold_badge = gold_os.get("isGoldBadge")

        seo_title = seo.get("title")
        seo_description = seo.get("description")
        seo_bottom = seo.get("bottomContent")

        out = {
            "shopinfo_status": "ok",
            "shopinfo_error_message": err_msg,

            "shopinfo_domain": shop_core.get("domain"),
            "shopinfo_shop_id": id_to_str(shop_core.get("shopID")),
            "shopinfo_name": shop_core.get("name"),
            "shopinfo_default_sort": shop_core.get("defaultSort"),

            "shopinfo_open_since": create_info.get("openSince"),
            "shopinfo_total_favorite": favorite.get("totalFavorite"),
            "shopinfo_already_favorited": favorite.get("alreadyFavorited"),
            "shopinfo_active_product": info.get("activeProduct"),

            "shopinfo_avatar": assets.get("avatar"),
            "shopinfo_cover": assets.get("cover"),
            "shopinfo_location": info.get("location"),
            "shopinfo_shipping_district": shipping_loc.get("districtName"),
            "shopinfo_shipping_city": shipping_loc.get("cityName"),

            "shopinfo_is_allow_manage": info.get("isAllowManage"),
            "shopinfo_is_open": info.get("isOpen"),
            "shopinfo_shop_status": status_info.get("shopStatus"),
            "shopinfo_status_message": status_info.get("statusMessage"),
            "shopinfo_status_title": status_info.get("statusTitle"),
            "shopinfo_ticker_type": status_info.get("tickerType"),

            "shopinfo_product_sold": stats.get("productSold"),
            "shopinfo_total_tx_success": stats.get("totalTxSuccess"),
            "shopinfo_total_showcase": stats.get("totalShowcase"),

            "shopinfo_is_gold": is_gold,
            "shopinfo_is_gold_badge": is_gold_badge,
            "shopinfo_is_official": is_official,
            "shopinfo_badge_url": badge_url,
            "shopinfo_badge_type": infer_badge_type(badge_url, is_official, is_gold, is_gold_badge),
            "shopinfo_shop_tier": gold_os.get("shopTier"),

            "shopinfo_shop_snippet_url": info.get("shopSnippetURL"),
            "shopinfo_custom_seo_title": seo_title,
            "shopinfo_custom_seo_description": seo_description,
            "shopinfo_custom_seo_bottom_text": strip_html(seo_bottom),

            "shopinfo_is_qa": info.get("isQA"),
            "shopinfo_is_goapotik": info.get("isGoApotik"),
            "shopinfo_partner_info_json": safe_json_dumps(partner),
            "shopinfo_bb_info_json": safe_json_dumps(bb_info),
            "shopinfo_shipment_info_json": safe_json_dumps(shipment_info),
            "shopinfo_raw_json": safe_json_dumps(root),
        }
        return out

    except Exception as e:
        base["shopinfo_status"] = f"parse_error:{type(e).__name__}"
        base["shopinfo_error_message"] = str(e)
        return base


# =========================================================
# 6) DATA PREP + CHECKPOINT
# =========================================================

def load_checkpoint(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_index": -1, "failed_indices": []}


def save_checkpoint(path: str, last_index: int, failed_indices: List[int]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_index": last_index, "failed_indices": failed_indices}, f, ensure_ascii=False, indent=2)


def build_unique_shop_table(input_csv: str) -> pd.DataFrame:
    # dtype=str penting agar ID tidak makin rusak saat dibaca Python.
    df = pd.read_csv(input_csv, dtype=str, low_memory=False)

    # Pastikan kolom ada.
    for col in ["shop_id", "shop_name", "shop_url", "url"]:
        if col not in df.columns:
            df[col] = ""

    df["shop_id"] = df["shop_id"].apply(id_to_str)
    df["shop_domain_from_shop_url"] = df["shop_url"].apply(extract_shop_domain_from_url)
    df["shop_domain_from_product_url"] = df["url"].apply(extract_shop_domain_from_url)
    df["shop_domain"] = df["shop_domain_from_shop_url"].where(
        df["shop_domain_from_shop_url"].astype(bool), df["shop_domain_from_product_url"]
    )

    # Toko unik: domain diprioritaskan karena ShopInfoCore bisa pakai domain dengan id=0.
    shops = (
        df[["shop_id", "shop_name", "shop_url", "shop_domain"]]
        .drop_duplicates()
        .copy()
    )

    # Hapus baris tanpa domain dan tanpa shop_id.
    shops = shops[(shops["shop_domain"].astype(str).str.len() > 0) | (shops["shop_id"].astype(str).str.len() > 0)]

    # Jika domain sama muncul berkali-kali, ambil satu.
    # Kalau domain kosong, fallback dedup by shop_id.
    shops["dedup_key"] = shops["shop_domain"].where(shops["shop_domain"].astype(bool), shops["shop_id"])
    shops = shops.drop_duplicates(subset=["dedup_key"]).reset_index(drop=True)
    shops.insert(0, "shop_row_index", shops.index)

    if ADD_EXCEL_SAFE_ID_COLS:
        shops["shop_id_excel_safe"] = shops["shop_id"].apply(excel_safe_id)

    return shops


SHOPINFO_OUTPUT_ORDER = [
    "shop_row_index",
    "shop_id", "shop_id_excel_safe", "shop_name", "shop_url", "shop_domain",
    "shopinfo_status", "shopinfo_error_message",
    "shopinfo_domain", "shopinfo_shop_id", "shopinfo_shop_id_excel_safe", "shopinfo_name",
    "shopinfo_open_since", "shopinfo_total_favorite", "shopinfo_active_product",
    "shopinfo_location", "shopinfo_shipping_district", "shopinfo_shipping_city",
    "shopinfo_product_sold", "shopinfo_total_tx_success", "shopinfo_total_showcase",
    "shopinfo_is_gold", "shopinfo_is_gold_badge", "shopinfo_is_official",
    "shopinfo_badge_url", "shopinfo_badge_type", "shopinfo_shop_tier",
    "shopinfo_avatar", "shopinfo_cover", "shopinfo_shop_snippet_url",
    "shopinfo_custom_seo_title", "shopinfo_custom_seo_description", "shopinfo_custom_seo_bottom_text",
    "shopinfo_is_open", "shopinfo_shop_status", "shopinfo_status_message", "shopinfo_status_title",
    "shopinfo_is_qa", "shopinfo_is_goapotik",
    "shopinfo_partner_info_json", "shopinfo_bb_info_json", "shopinfo_shipment_info_json",
    "shopinfo_raw_json",
]


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in SHOPINFO_OUTPUT_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in cols and c != "dedup_key"]
    return df[cols + rest]


# =========================================================
# 7) MAIN
# =========================================================

def enrich_shopinfo_core(
    input_csv: str,
    output_csv: str,
    checkpoint_path: str,
    limit: Optional[int] = LIMIT,
    save_every: int = SAVE_EVERY,
) -> pd.DataFrame:
    if os.path.abspath(input_csv) == os.path.abspath(output_csv):
        raise ValueError("INPUT_CSV dan OUTPUT_CSV tidak boleh sama.")

    shops = build_unique_shop_table(input_csv)

    # Tambahkan kolom status jika belum ada.
    for col in ["shopinfo_status", "shopinfo_error_message"]:
        if col not in shops.columns:
            shops[col] = None

    ckpt = load_checkpoint(checkpoint_path)
    start = int(ckpt.get("last_index", -1)) + 1
    failed_indices = list(ckpt.get("failed_indices", []))
    end = len(shops) if limit is None else min(len(shops), start + limit)

    print(f"[INFO] unique shops total = {len(shops)}")
    print(f"[START] ShopInfoCore enrichment shop index {start} sampai {end - 1}")

    session = requests.Session()
    try:
        session.get("https://www.tokopedia.com/", headers={"user-agent": SHOPINFO_CFG["headers"]["user-agent"]}, timeout=30)
    except Exception:
        pass

    pr = PoliteRequester()

    processed_rows: List[pd.DataFrame] = []

    # Kalau output sudah ada dan checkpoint lanjut, append result baru ke output lama.
    # Untuk menghindari output berisi semua old data, kita hanya append row yang diproses.
    output_exists = os.path.exists(output_csv) and start > 0

    for i in range(start, end):
        row = shops.iloc[i].copy()
        domain = safe_str(row.get("shop_domain"))
        shop_id = safe_str(row.get("shop_id"))

        try:
            if not domain and not shop_id:
                row["shopinfo_status"] = "skip_no_domain_no_shop_id"
            else:
                root = fetch_shopinfo_core(pr, session, domain=domain, shop_id=shop_id)
                features = flatten_shopinfo_response(root)
                for k, v in features.items():
                    row[k] = v

                if ADD_EXCEL_SAFE_ID_COLS:
                    row["shopinfo_shop_id_excel_safe"] = excel_safe_id(row.get("shopinfo_shop_id"))

            row_df = pd.DataFrame([row.to_dict()])
            row_df = order_columns(row_df)
            processed_rows.append(row_df)

        except Exception as e:
            print(f"[ERROR] shop_index={i} domain={domain!r} shop_id={shop_id!r}: {type(e).__name__}: {e}")
            row["shopinfo_status"] = f"error:{type(e).__name__}"
            row["shopinfo_error_message"] = str(e)
            failed_indices.append(i)
            row_df = order_columns(pd.DataFrame([row.to_dict()]))
            processed_rows.append(row_df)

        # Save incremental.
        if ((i + 1) % save_every == 0) or (i == end - 1):
            batch = pd.concat(processed_rows, ignore_index=True) if processed_rows else pd.DataFrame()
            if not batch.empty:
                os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
                write_header = not os.path.exists(output_csv) or not output_exists
                batch.to_csv(output_csv, mode="a", header=write_header, index=False, encoding="utf-8-sig")
                output_exists = True
                processed_rows = []

            save_checkpoint(checkpoint_path, i, failed_indices)
            print(f"[SAVE] progress shop_index={i} -> {output_csv}")

    print(f"[DONE] saved {output_csv}")
    print(f"[NOTE] Untuk mulai ulang dari awal, hapus file output dan checkpoint:")
    print(f"       {output_csv}")
    print(f"       {checkpoint_path}")
    return shops


if __name__ == "__main__":
    enrich_shopinfo_core(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        checkpoint_path=CHECKPOINT_PATH,
        limit=LIMIT,
        save_every=SAVE_EVERY,
    )
