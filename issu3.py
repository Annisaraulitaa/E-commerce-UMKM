import json
import time
import random
import requests
from urllib.parse import parse_qs

URL = "https://gql.tokopedia.com/graphql/SearchProductV5Query"

headers = {
    "accept": "*/*",
    "bd-device-id": "7527216031573837313",
    "bd-web-id": "7527216031573837313",
    "content-type": "application/json",
    "iris_session_id": "",          # kalau ada, isi
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "tkpd-userid": "",              # kalau ada, isi
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-dark-mode": "false",
    "x-device": "desktop-0.0",
    "x-price-center": "true",
    "x-source": "tokopedia-lite",
    "x-tkpd-lite-service": "zeus",
}

QUERY = """query SearchProductV5Query($params: String!) {
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
            banner { position text applink url imageURL componentID trackingOption __typename }
            redirection { url __typename }
            related {
                relatedKeyword
                position
                trackingOption
                otherRelated {
                    keyword url applink componentID
                    products {
                        oldID: id
                        id: id_str_auto_
                        name
                        url
                        applink
                        mediaURL { image __typename }
                        shop { oldID: id id: id_str_auto_ name city tier __typename }
                        badge { oldID: id id: id_str_auto_ title url __typename }
                        price { text number __typename }
                        freeShipping { url __typename }
                        labelGroups {
                            position title type url
                            styles { key value __typename }
                            __typename
                        }
                        rating
                        wishlist
                        ads { id productClickURL productViewURL productWishlistURL tag __typename }
                        meta { oldWarehouseID: warehouseID warehouseID: warehouseID_str_auto_ componentID __typename }
                        __typename
                    }
                __typename
            }
        __typename
    }
    suggestion { currentKeyword suggestion query text componentID trackingOption __typename }
    ticker { oldID: id id: id_str_auto_ text query applink componentID trackingOption __typename }
    violation {
        headerText descriptionText imageURL ctaURL ctaApplink buttonText buttonType __typename
    }
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
            position title type url
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
}"""


BASE_PARAMS = "device=desktop&enter_method=normal_search&l_name=sre&navsource=&ob=23&page=1&q=baju&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=ab63b850c54e964212ffea525b4d8a19&user_addressId=&user_cityId=176&user_districtId=2274&user_id=&user_lat=&user_long=&user_postCode=&user_warehouseId=&variants=&warehouses="

# ====== TARGET SETTINGS ======
MAX_PRODUCTS = 320       # target ambil sampai totalData 320
MAX_REQUESTS = 20        # cukup untuk 320 (sekitar 6 batch), kasih ruang
ROWS = 60
MIN_DELAY = 2.0
MAX_DELAY = 4.0
MAX_RETRIES_5XX = 2
CHECKPOINT_FILE = "checkpoint_search.json"
OUTPUT_FILE = "search_products.json"

# kalau True: selalu mulai dari awal (disarankan kalau mau ambil full 320)
RESET_CHECKPOINT = True


def parse_params_string(params_str: str) -> dict:
    return {k: v[0] for k, v in parse_qs(params_str, keep_blank_values=True).items()}

def build_params(base_params: str, *, q: str, page: int, start: int, rows: int) -> str:
    d = parse_params_string(base_params)
    d["q"] = q
    d["page"] = str(page)      # tetap 1
    d["start"] = str(start)    # pagination pakai offset
    d["rows"] = str(rows)
    return "&".join([f"{k}={d[k]}" for k in d.keys()])

def extract_products_and_meta(resp_json):
    if isinstance(resp_json, list) and resp_json:
        resp_json = resp_json[0]

    sp = (resp_json.get("data") or {}).get("searchProductV5") or {}
    header = sp.get("header") or {}
    body = sp.get("data") or {}

    products = body.get("products") or []

    additional = header.get("additionalParams") or ""
    qs = parse_qs(additional)

    has_more = (qs.get("has_more", ["false"])[0].lower() == "true")
    next_offset = int(qs.get("next_offset_organic", [0])[0] or 0)

    meta = {
        "totalData": header.get("totalData"),
        "responseCode": header.get("responseCode"),
        "isQuerySafe": header.get("isQuerySafe"),
        "has_more": has_more,
        "next_offset_organic": next_offset,
        "additionalParams": additional,
    }
    return products, meta

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def save_checkpoint(state: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def polite_sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def fetch_search_products_safe(keyword: str):
    session = requests.Session()

    # ===== checkpoint =====
    if RESET_CHECKPOINT:
        ck = {}
    else:
        ck = load_checkpoint() or {}

    start = int(ck.get("start", 0))
    page = 1  # PAKSA page selalu 1
    seen = set(ck.get("seen_ids", []))
    all_products = ck.get("products", [])

    req_count = 0

    while req_count < MAX_REQUESTS and len(all_products) < MAX_PRODUCTS:
        params = build_params(BASE_PARAMS, q=keyword, page=1, start=start, rows=ROWS)

        payload = {
            "operationName": "SearchProductV5Query",
            "variables": {"params": params},
            "query": QUERY
        }

        last_resp = None
        for attempt in range(1, MAX_RETRIES_5XX + 2):
            try:
                resp = session.post(URL, headers=headers, json=payload, timeout=(5, 25))
                last_resp = resp
            except requests.RequestException as e:
                if attempt >= MAX_RETRIES_5XX + 1:
                    print(f"[start {start}] request error: {e} -> stop.")
                    return all_products
                time.sleep(1.5 * attempt)
                continue

            if resp.status_code in (401, 403, 429):
                print(f"[start {start}] HTTP {resp.status_code} -> stop (jangan diulang).")
                return all_products

            if resp.status_code in (500, 502, 503, 504):
                if attempt >= MAX_RETRIES_5XX + 1:
                    print(f"[start {start}] HTTP {resp.status_code} -> stop.")
                    return all_products
                time.sleep(1.5 * attempt)
                continue

            if resp.status_code != 200:
                print(f"[start {start}] HTTP {resp.status_code}: {resp.text[:200]} -> stop.")
                return all_products

            break

        req_count += 1

        try:
            j = last_resp.json()
        except ValueError:
            print(f"[start {start}] non-JSON response -> stop.")
            return all_products

        products, meta = extract_products_and_meta(j)

        # kalau products kosong, langsung stop (jangan maksa)
        if not products:
            print(f"[req {req_count}] products kosong -> stop. meta: {meta}")
            break

        new_count = 0
        for p in products:
            pid = p.get("id") or p.get("ttsProductID")
            if pid and pid in seen:
                continue
            if pid:
                seen.add(pid)
            all_products.append(p)
            new_count += 1
            if len(all_products) >= MAX_PRODUCTS:
                break

        print(
            f"[req {req_count}] start={start} got={len(products)} new={new_count} total={len(all_products)} "
            f"has_more={meta['has_more']} next_start={meta['next_offset_organic']} totalData={meta['totalData']}"
        )

        save_checkpoint({
            "keyword": keyword,
            "start": meta["next_offset_organic"],
            "seen_ids": list(seen),
            "products": all_products
        })

        if new_count == 0 or not meta["has_more"]:
            break

        start = meta["next_offset_organic"]
        polite_sleep()

    return all_products


# ===== RUN =====
products = fetch_search_products_safe("baju")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(products, f, ensure_ascii=False, indent=2)

print("✅ Selesai. Total produk tersimpan:", len(products))
print(f"Checkpoint tersimpan di: {CHECKPOINT_FILE}")
