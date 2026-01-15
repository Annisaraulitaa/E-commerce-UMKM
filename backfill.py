# backfill_price_original.py
import os, json, time, random
from typing import Dict, Any, List, Tuple
from urllib.parse import parse_qsl, urlencode

import requests
import pandas as pd


# =========================================================
# CFG (sesuaikan kalau perlu)
# =========================================================

BD_DEVICE_ID = "7527216031573837313"

SEARCH_CFG = {
    "url": "https://gql.tokopedia.com/graphql/SearchProductV5Query",
    "headers": {
        "accept": "*/*",
        "bd-device-id": BD_DEVICE_ID,
        "bd-web-id": BD_DEVICE_ID,
        "content-type": "application/json",
        "referer": "https://www.tokopedia.com/",
        "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-dark-mode": "false",
        "x-device": "desktop-0.0",
        "x-price-center": "true",
        "x-source": "tokopedia-lite",
        "x-tkpd-lite-service": "zeus",
    },
    "operationName": "SearchProductV5Query",
    "query": """query SearchProductV5Query($params: String!) {
        searchProductV5(params: $params) {
            header {
                totalData
                responseCode
                additionalParams
                backendFilters
                __typename
            }
            data {
                products {
                    id: id_str_auto_
                    price { number original discountPercentage text __typename }
                    __typename
                }
            __typename
            }
        __typename
        }
    }
    """,
    # kamu boleh ganti ini dari DevTools; keyword akan dioverride oleh argumen `keyword`
    "params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=&ob=23&page=1&q=hiasan%20rumah%20handmade&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=ab63b850c54e964212ffea525b4d8a19&user_addressId=&user_cityId=176&user_districtId=2274&user_id=&user_lat=&user_long=&user_postCode=&user_warehouseId=&variants=&warehouses=",
}


# =========================================================
# Polite requester (delay + backoff)
# =========================================================

class PoliteRequester:
    def __init__(
        self,
        min_delay: float = 1.5,
        max_delay: float = 3.5,
        long_break_every: int = 80,
        long_break_range: Tuple[float, float] = (30, 90),
        max_retries: int = 6,
        backoff_base: float = 2.0,
        backoff_cap: float = 120.0,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.long_break_every = long_break_every
        self.long_break_range = long_break_range
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.req_count = 0

    def _sleep_polite(self):
        time.sleep(random.uniform(self.min_delay, self.max_delay))
        self.req_count += 1
        if self.long_break_every and (self.req_count % self.long_break_every == 0):
            lb = random.uniform(*self.long_break_range)
            print(f"[PAUSE] long break {lb:.1f}s (after {self.req_count} requests)")
            time.sleep(lb)

    def post_json(self, session: requests.Session, url: str, headers: Dict[str, str], payload: Any, timeout=30) -> Any:
        for attempt in range(self.max_retries + 1):
            self._sleep_polite()
            try:
                r = session.post(url, headers=headers, json=payload, timeout=timeout)

                if r.status_code in (429, 403, 500, 502, 503, 504):
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        wait = min(float(ra), self.backoff_cap)
                    else:
                        wait = min((self.backoff_base ** attempt) + random.uniform(0, 1.0), self.backoff_cap)
                    print(f"[BACKOFF] HTTP {r.status_code} attempt={attempt+1}/{self.max_retries+1} wait={wait:.1f}s")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                wait = min((self.backoff_base ** attempt) + random.uniform(0, 1.0), self.backoff_cap)
                print(f"[RETRY] {type(e).__name__} attempt={attempt+1}/{self.max_retries+1} wait={wait:.1f}s")
                time.sleep(wait)

        raise RuntimeError("Max retries exceeded")


# =========================================================
# Helpers: params, additionalParams
# =========================================================

def first_item_if_list(x: Any) -> Any:
    return x[0] if isinstance(x, list) and x else x

def parse_additional_params(s: str) -> dict:
    if not s:
        return {}
    return dict(parse_qsl(s, keep_blank_values=True))

def build_search_params(params_template: str, keyword: str, page: int, carry: dict) -> str:
    d = dict(parse_qsl(params_template, keep_blank_values=True))

    d["q"] = keyword
    # Banyak kasus: paging di SearchProductV5 pakai start (offset), page tetap 1
    d["page"] = "1"

    rows = int(d.get("rows", "60") or "60")

    # start offset: pakai dari server bila ada, kalau tidak hitung manual
    if carry and carry.get("next_offset_organic"):
        d["start"] = str(carry["next_offset_organic"])
    else:
        d["start"] = str((page - 1) * rows)

    # hanya kirim search_id kalau ada
    if carry.get("search_id"):
        d["search_id"] = str(carry["search_id"])
    else:
        d.pop("search_id", None)

    return urlencode(d, doseq=True)


# =========================================================
# Fetch search page
# =========================================================

def fetch_search_page(pr: PoliteRequester, session: requests.Session, keyword: str, page: int, carry: dict):
    params = build_search_params(SEARCH_CFG["params_template"], keyword, page, carry)
    print(f"[DEBUG] page={page} params={params}")

    payload = [{
        "operationName": SEARCH_CFG["operationName"],
        "variables": {"params": params},
        "query": SEARCH_CFG["query"],
    }]

    resp = pr.post_json(session, SEARCH_CFG["url"], SEARCH_CFG["headers"], payload)
    root = first_item_if_list(resp)

    sp = root.get("data", {}).get("searchProductV5", {})
    header = sp.get("header", {}) or {}
    products = (sp.get("data", {}) or {}).get("products", []) or []

    carry_next = parse_additional_params(header.get("additionalParams", "") or "")
    totalData = header.get("totalData")
    responseCode = header.get("responseCode")

    return products, carry_next, totalData, responseCode


# =========================================================
# Backfill state
# =========================================================

def load_state(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"page": 1, "carry": {}}

def save_state(path: str, page: int, carry: dict):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"page": page, "carry": carry}, f, ensure_ascii=False)


# =========================================================
# Main backfill
# =========================================================

def backfill_price_original(
    csv_in: str,
    csv_out: str,
    keyword: str,
    state_path: str,
    max_pages: int = 9999,
    page_pause_range: Tuple[float, float] = (5, 12),
):
    # Load CSV
    df = pd.read_csv(csv_in, dtype={"id": str})

    if "id" not in df.columns:
        raise ValueError("CSV input tidak punya kolom 'id'.")

    if "price_original" not in df.columns:
        df["price_original"] = pd.NA

    # id yang perlu diisi
    s = df["price_original"].astype("string")
    missing_mask = s.isna() | (s.str.strip() == "") | (s.str.lower() == "nan")
    need_ids = set(df.loc[missing_mask, "id"].astype(str).tolist())

    print(f"[BACKFILL] rows={len(df)} missing price_original={len(need_ids)}")
    if not need_ids:
        print("[BACKFILL] Tidak ada yang perlu diisi. Menyimpan output saja.")
        os.makedirs(os.path.dirname(csv_out) or ".", exist_ok=True)
        df.to_csv(csv_out, index=False, encoding="utf-8-sig")
        return

    # Session warm-up
    session = requests.Session()
    session.get("https://www.tokopedia.com/", headers={"user-agent": SEARCH_CFG["headers"]["user-agent"]}, timeout=30)

    pr = PoliteRequester(
        min_delay=1.5,
        max_delay=3.5,
        long_break_every=80,
        long_break_range=(30, 90),
        max_retries=6,
    )

    st = load_state(state_path)
    start_page = int(st.get("page", 1))
    carry = st.get("carry", {}) or {}

    if start_page <= 1:
        carry = {}

    filled = 0
    os.makedirs(os.path.dirname(csv_out) or ".", exist_ok=True)

    for page in range(start_page, max_pages + 1):
        try:
            prods, carry_next, totalData, responseCode = fetch_search_page(pr, session, keyword, page, carry)
        except Exception as e:
            print(f"[ERROR] search page {page}: {e}")
            save_state(state_path, page, carry)
            break

        if responseCode not in (0, None):
            print(f"[WARN] responseCode={responseCode} di page {page}")
            print(f"[WARN] carry_next={carry_next}")

        if not prods:
            print(f"[STOP] page {page} kosong")
            save_state(state_path, page, carry_next)
            break

        hit = 0
        for p in prods:
            pid = str(p.get("id"))
            if pid in need_ids:
                original = (p.get("price") or {}).get("original")
                if original is not None and str(original).strip() != "":
                    df.loc[df["id"].astype(str) == pid, "price_original"] = original
                    need_ids.remove(pid)
                    filled += 1
                    hit += 1

        print(f"[BACKFILL] page {page} hits={hit} filled_total={filled} remaining={len(need_ids)} totalData={totalData}")

        # save progress setiap page
        df.to_csv(csv_out, index=False, encoding="utf-8-sig")
        carry = carry_next
        save_state(state_path, page + 1, carry)

        if not need_ids:
            print("[DONE] Semua price_original sudah terisi.")
            break

        if str(carry.get("has_more", "")).lower() == "false":
            print("[STOP] has_more=false (server bilang hasil search habis).")
            break

        pause = random.uniform(*page_pause_range)
        print(f"[PAUSE] between pages {pause:.1f}s")
        time.sleep(pause)

    # final save
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"[BACKFILL] Saved: {csv_out}")
    if need_ids:
        print(f"[BACKFILL] WARNING: masih ada {len(need_ids)} id yang belum ketemu di hasil search (normal kalau ranking berubah).")


# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    # GANTI SESUAI FILE KAMU
    csv_in = "output/hiasanRumah_enriched.csv"                          #ini diganti
    csv_out = "output/hiasanRumah_enriched_backfilled.csv"              #ini diganti
    keyword = "Hiasan Rumah"                                            #ini diganti
    state_path = "output/state_backfill_hiasanRumah.json"               #ini diganti

    backfill_price_original(
        csv_in=csv_in,
        csv_out=csv_out,
        keyword=keyword,
        state_path=state_path,
        max_pages=9999,
        page_pause_range=(5, 12),
    )
