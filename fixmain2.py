import os, json, time, random, re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode

import requests
import pandas as pd


# =========================================================
# 0) CFG
# =========================================================

BD_DEVICE_ID = "7527216031573837313"

REVIEW_CFG = {
    "url": "https://gql.tokopedia.com/graphql/productRatingAndTopics",
    "headers": {
        "accept": "*/*",
        "bd-device-id": BD_DEVICE_ID,
        "content-type": "application/json",
        "referer": "https://www.tokopedia.com/",
        "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-price-center": "true",
        "x-source": "tokopedia-lite",
        "x-theme": "default",
        "x-tkpd-lite-service": "zeus",
    },
    "operationName": "productRatingAndTopics",
    "query": """query productRatingAndTopics($productID: String!) {
    productrevGetProductRatingAndTopics(productID: $productID) {
    productID
    rating {
        positivePercentageFmt
        ratingScore
        totalRating
        totalRatingWithImage
        totalRatingTextAndImage
        detail {
            rate
            totalReviews
            formattedTotalReviews
            percentageFloat
            __typename
        }
        isAggregatedWithTTS
        __typename
        }
    topics {
        rating
        ratingFmt
        formatted
        key
        reviewCount
        reviewCountFmt
        show
        __typename
        }
    availableFilters {
        withAttachment
        rating
        topics
        helpfulness
        __typename
        }
    layout {
        backgroundColor
        reviewSourceText
        reviewSourceIconUrl
        __typename
        }
    __typename
    }
}
""",
}

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
        keywordProcess
        keywordIntention
        componentID
        isQuerySafe
        additionalParams
        backendFilters
        meta { dynamicFields __typename }
        __typename
    }
    data {
        totalDataText
        products {
            oldID: id
            id: id_str_auto_
            ttsProductID
            name
            url
            applink
            mediaURL { image image300 videoCustom __typename }
            shop {
                oldID: id
                id: id_str_auto_
                ttsSellerID
                name
                url
                city
                tier
                __typename
            }
            stock { ttsSKUID __typename }
            badge { oldID: id id: id_str_auto_ title url __typename }
            price { text number range original discountPercentage __typename }
            freeShipping { url __typename }
            labelGroups {
                position
                title
                type
                url
                styles { key value __typename }
                __typename
            }
            labelGroupsVariant { title type typeVariant hexColor __typename }
            category { oldID: id id: id_str_auto_ name breadcrumb gaKey __typename }
            rating
            wishlist
            ads { id productClickURL productViewURL productWishlistURL tag __typename }
            meta {
                oldParentID: parentID
                parentID: parentID_str_auto_
                oldWarehouseID: warehouseID
                warehouseID: warehouseID_str_auto_
                isImageBlurred
                isPortrait
                __typename
                }
            __typename
            }
        __typename
        }
    __typename
    }
}
""",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=kerajinan%20anyaman&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=hiasan%20rumah&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    "params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=perlengkapan%20bayi&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #params_template nya ganti kalau mau scraping keyword lain
}

OUTPUT_COLS = [
    "id","name","url","category_breadcrumb",
    "price_number","price_original","discountPercentage",
    "mediaURL_image","mediaURL_image_fixed",
    "ratingAverage",
    "shop_id","shop_name","shop_url","shop_city","shop_tier",
    "countSold","isTopAds","labelGroups", "label_titles",
    "totalRating","countReview",
    "image_status","image_source","image_local_path",
]


# =========================================================
# 1) POLITE REQUESTER (THROTTLE + BACKOFF + LONG BREAK)
# =========================================================

class PoliteRequester:
    def __init__(
        self,
        min_delay: float = 1.2,
        max_delay: float = 3.0,
        long_break_every: int = 100,
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
# 2) HELPERS: PARAMS PAGINATION (additionalParams), parse sold, topads
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
    d["page"] = "1"

    rows = int(d.get("rows", "60") or "60")

    if carry and carry.get("next_offset_organic"):
        d["start"] = str(carry["next_offset_organic"])
    else:
        d["start"] = str((page - 1) * rows)

    if carry.get("search_id"):
        d["search_id"] = str(carry["search_id"])
    else:
        d.pop("search_id", None)

    return urlencode(d, doseq=True)

def parse_count_sold_from_labelgroups(label_groups) -> Optional[int]:
    if not isinstance(label_groups, list):
        return None

    chosen = None
    for lg in label_groups:
        if (lg or {}).get("position") == "ri_product_credibility":
            t = (lg or {}).get("title", "") or ""
            if "terjual" in t.lower():
                chosen = t
                break
    if not chosen:
        for lg in label_groups:
            t = (lg or {}).get("title", "") or ""
            if "terjual" in t.lower():
                chosen = t
                break
    if not chosen:
        return None

    s = chosen.lower().replace("terjual", "").replace("+", "").strip()

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(rb|ribu|jt|juta|k|m)?", s)
    if not m:
        return None

    num_str = m.group(1)
    unit = (m.group(2) or "").lower()

    num_float = float(num_str.replace(",", "."))

    if unit in ("rb", "ribu", "k"):
        return int(num_float * 1_000)
    if unit in ("jt", "juta", "m"):
        return int(num_float * 1_000_000)

    clean = re.sub(r"[.,]", "", num_str)
    return int(clean) if clean.isdigit() else None

def is_topads_from_ads(ads: dict) -> bool:
    if not isinstance(ads, dict):
        return False
    if (ads.get("tag") or 0) != 0:
        return True
    if (ads.get("id") or "").strip():
        return True
    for k in ("productClickURL", "productViewURL", "productWishlistURL"):
        if (ads.get(k) or "").strip():
            return True
    return False


# =========================================================
# 2b) IMAGE HELPERS: fix url, refresh from product page, download local
# =========================================================

IMG_RE = re.compile(r"https://[^\"'<> ]+tokopedia-static\.net[^\"'<> ]+\.(?:jpg|jpeg|png|webp)[^\"'<> ]*")
UI_BAD_KEYWORDS = ["assets-tokopedia-lite", "icon", "logo", "sprite", "favicon", "/prod/", "/assets/", "/static/"]

def fix_img_url(u: str) -> str:
    if not isinstance(u, str):
        return ""
    u = u.replace("\\u0026", "&")
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    return u

def is_ui_asset(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in UI_BAD_KEYWORDS)

def score_product_like(url: str) -> int:
    u = url.lower()
    score = 0
    if "/img/" in u: score += 5
    if "tos-alisg" in u: score += 3
    if "aphluv4xwc" in u: score += 2
    if "~tplv-" in u: score += 2
    if is_ui_asset(u): score -= 100
    return score

def extract_img_candidates(html: str) -> List[str]:
    urls = IMG_RE.findall(html)
    seen, out = set(), []
    for u in urls:
        u = fix_img_url(u)
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    out = [u for u in out if not is_ui_asset(u)]
    out.sort(key=score_product_like, reverse=True)
    return out

def refresh_image_from_product_page(session: requests.Session, product_url: str, page_headers: dict) -> Optional[str]:
    try:
        r = session.get(product_url, headers=page_headers, timeout=30)
        if r.status_code != 200:
            return None
        candidates = extract_img_candidates(r.text)
        return candidates[0] if candidates else None
    except requests.RequestException:
        return None

def download_image(session: requests.Session, img_url: str, out_path: str, img_headers: dict, min_bytes: int = 20_000) -> Tuple[bool, int]:
    try:
        rr = session.get(img_url, headers=img_headers, timeout=30)
        status = rr.status_code
        ct = rr.headers.get("content-type", "")
        if status == 200 and ct.startswith("image/") and len(rr.content) >= min_bytes:
            with open(out_path, "wb") as f:
                f.write(rr.content)
            return True, status
        return False, status
    except requests.RequestException:
        return False, -1


# =========================================================
# 3) FETCHERS: SEARCH + REVIEW
# =========================================================

def flatten_search_product(p: Dict[str, Any]) -> Dict[str, Any]:
    label_groups = p.get("labelGroups") or []
    ads = p.get("ads") or {}

    img_raw = ((p.get("mediaURL") or {}).get("image")) or ""
    img_fixed = fix_img_url(img_raw) if img_raw else ""

    return {
        "id": p.get("id"),
        "name": p.get("name"),
        "url": p.get("url"),
        "category_breadcrumb": (p.get("category") or {}).get("breadcrumb"),
        "price_number": ((p.get("price") or {}).get("number")),
        "price_original": ((p.get("price") or {}).get("original")),
        "discountPercentage": ((p.get("price") or {}).get("discountPercentage")),
        "mediaURL_image": img_raw,
        "mediaURL_image_fixed": img_fixed,
        "ratingAverage": float(p.get("rating")) if p.get("rating") not in (None, "") else None,
        "shop_id": ((p.get("shop") or {}).get("id")),
        "shop_name": ((p.get("shop") or {}).get("name")),
        "shop_url": ((p.get("shop") or {}).get("url")),
        "shop_city": ((p.get("shop") or {}).get("city")),
        "shop_tier": ((p.get("shop") or {}).get("tier")),
        "countSold": parse_count_sold_from_labelgroups(label_groups),
        "isTopAds": is_topads_from_ads(ads),
        "labelGroups": json.dumps(label_groups, ensure_ascii=False),
        "label_titles": " | ".join(
            lg.get("title", "")
            for lg in label_groups
            if isinstance(lg, dict)
        ),
        # image fields (filled later)
        "image_status": None,
        "image_source": None,
        "image_local_path": None,
    }

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

    header = root["data"]["searchProductV5"]["header"]
    products = root["data"]["searchProductV5"]["data"]["products"] or []
    carry_next = parse_additional_params(header.get("additionalParams", ""))

    return products, carry_next, header.get("totalData"), header.get("responseCode")

def fetch_review_summary(pr: PoliteRequester, session: requests.Session, product_id: str):
    payload = [{
        "operationName": REVIEW_CFG["operationName"],
        "variables": {"productID": str(product_id)},
        "query": REVIEW_CFG["query"],
    }]

    resp = pr.post_json(session, REVIEW_CFG["url"], REVIEW_CFG["headers"], payload)
    root = first_item_if_list(resp)

    rating = root["data"]["productrevGetProductRatingAndTopics"]["rating"]
    total_rating = rating.get("totalRating")
    count_review = rating.get("totalRatingTextAndImage")
    rating_avg = float(rating["ratingScore"]) if rating.get("ratingScore") else None

    return total_rating, count_review, rating_avg


# =========================================================
# 4) CHECKPOINT + SAVE BERTAHAP
# =========================================================

def load_state(state_path: str) -> Dict[str, Any]:
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"page": 1, "seen_ids": [], "carry": {}}

def save_state(state_path: str, page: int, seen_ids: set, carry: dict):
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump({"page": page, "seen_ids": list(seen_ids), "carry": carry}, f, ensure_ascii=False)

def append_rows_to_csv(csv_path: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    df = pd.DataFrame(rows).reindex(columns=OUTPUT_COLS)
    write_header = not os.path.exists(csv_path)
    df.to_csv(csv_path, mode="a", header=write_header, index=False)


# =========================================================
# 5) RUNNER: BERTAHAP + STOP KALAU has_more=false
# =========================================================

def run_scrape_enrich_batched(
    keyword: str,
    max_pages: int = 9999,
    per_run_page_limit: int = 5,
    out_csv: str = "tokopedia_enriched.csv",
    state_path: str = "state.json",
    page_pause_range: Tuple[float, float] = (8, 20),
):
    print("[START] running scrape...")

    session = requests.Session()
    session.get("https://www.tokopedia.com/", headers={"user-agent": SEARCH_CFG["headers"]["user-agent"]}, timeout=30)

    pr = PoliteRequester(
        min_delay=2.0,
        max_delay=5.0,
        long_break_every=60,
        long_break_range=(60, 180),
        max_retries=6,
    )

    # folder gambar berdasar nama CSV
    base_dir = os.path.dirname(out_csv) or "."
    base_name = os.path.splitext(os.path.basename(out_csv))[0]
    img_dir = os.path.join(base_dir, f"{base_name}_images")
    os.makedirs(img_dir, exist_ok=True)

    PAGE_HEADERS_HTML = {
        "User-Agent": SEARCH_CFG["headers"]["user-agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    IMG_HEADERS_CDN = {
        "User-Agent": SEARCH_CFG["headers"]["user-agent"],
        "Referer": "https://www.tokopedia.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }

    state = load_state(state_path)
    start_page = int(state.get("page", 1))
    seen_ids = set(state.get("seen_ids", []))
    carry = state.get("carry", {}) or {}

    if start_page <= 1:
        carry = {}

    pages_done = 0

    for page in range(start_page, max_pages + 1):
        try:
            prods, carry_next, totalData, responseCode = fetch_search_page(pr, session, keyword, page, carry)
        except Exception as e:
            print(f"[ERROR] search page {page}: {e}")
            save_state(state_path, page, seen_ids, carry)
            break

        if responseCode not in (0, None):
            print(f"[WARN] responseCode={responseCode} di page {page}")
            print(f"[WARN] carry_next={carry_next}")

        if not prods:
            print(f"[STOP] page {page} kosong")
            save_state(state_path, page, seen_ids, carry_next)
            break

        rows_to_save = []

        for p in prods:
            row = flatten_search_product(p)
            pid = row.get("id")
            if not pid or pid in seen_ids:
                continue

            # ====== IMAGE: download NOW + refresh fallback ======
            pid_str = str(pid)
            signed_img = row.get("mediaURL_image_fixed") or ""
            local_path = os.path.join(img_dir, f"{pid_str}.jpg")

            # jeda kecil biar sopan (gambar + html bukan via pr.post_json)
            time.sleep(random.uniform(0.3, 0.9))

            if signed_img:
                ok_img, st = download_image(session, signed_img, local_path, IMG_HEADERS_CDN)
                row["image_status"] = st
                if ok_img:
                    row["image_source"] = "signed"
                    row["image_local_path"] = local_path
                else:
                    refreshed = refresh_image_from_product_page(session, row["url"], PAGE_HEADERS_HTML)
                    if refreshed:
                        time.sleep(random.uniform(0.3, 0.9))
                        ok2, st2 = download_image(session, refreshed, local_path, IMG_HEADERS_CDN)
                        row["image_status"] = st2
                        if ok2:
                            row["image_source"] = "refreshed"
                            row["image_local_path"] = local_path

            # ====== REVIEW enrich ======
            try:
                totalRating, countReview, ratingAvgFromPDP = fetch_review_summary(pr, session, pid_str)
                row["totalRating"] = totalRating
                row["countReview"] = countReview
                if ratingAvgFromPDP is not None:
                    row["ratingAverage"] = ratingAvgFromPDP
            except Exception:
                row["totalRating"] = None
                row["countReview"] = None

            seen_ids.add(pid_str)
            rows_to_save.append(row)

        append_rows_to_csv(out_csv, rows_to_save)

        carry = carry_next
        save_state(state_path, page + 1, seen_ids, carry)

        print(f"[SAVE] page {page} appended={len(rows_to_save)} total_seen={len(seen_ids)} / totalData={totalData} -> {out_csv}")

        if str(carry.get("has_more", "")).lower() == "false":
            print("[STOP] has_more=false (server bilang hasil sudah habis)")
            break

        pause = random.uniform(*page_pause_range)
        print(f"[PAUSE] between pages {pause:.1f}s")
        time.sleep(pause)

        pages_done += 1
        if pages_done >= per_run_page_limit:
            print("[DONE] stop bertahap. Jalankan lagi untuk lanjut (resume dari state).")
            break


# =========================================================
# CONTOH PAKAI
# =========================================================
if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)

    run_scrape_enrich_batched(
        keyword="Perlengkapan Bayi",                                # ganti kalau mau scraping lagi
        per_run_page_limit=1,
        out_csv="output/perlengkapanBayi_enriched.csv",             # ini juga ganti
        state_path="output/state_perlengkapanBayi.json",            # ini juga ganti
        page_pause_range=(8, 20),
    )
