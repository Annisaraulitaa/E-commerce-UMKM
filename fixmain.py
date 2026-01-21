import os, json, time, random, re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode

import requests
import pandas as pd


# =========================================================
# 0) CFG DARI KAMU (SUDAH DIMASUKKAN)
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
    #1"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=&ob=23&page=1&q=hiasan%20rumah%20handmade&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=ab63b850c54e964212ffea525b4d8a19&user_addressId=&user_cityId=176&user_districtId=2274&user_id=&user_lat=&user_long=&user_postCode=&user_warehouseId=&variants=&warehouses=",
    #2"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home&navsource=home&ob=23&page=1&q=baju%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=celana%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=jaket%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=aksesoris%20laki%20laki&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=sepatu%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=sandal%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=tas%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=pakaian%20muslim%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=pakaian%20dalam%20pria&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=baju%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=celana%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=outer%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=pakaian%20muslim%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=jilbab%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=sendal%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=sepatu%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=aksesori%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=tas%20wanita&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=pakaian%20anak%20cowo&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=pakaian%20anak%20cewe&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=aksesori%20anak&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=makanan%20&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    #"params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=khas%20daerah&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
    "params_template": "device=desktop&enter_method=normal_search&l_name=sre&navsource=home%2Chome&ob=23&page=1&q=kain%20khas%20daerah&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=afccec9110b129d185c23a520a2ccc8c&user_addressId=&user_cityId=176&user_districtId=2274&user_id=235236327&user_lat=&user_long=&user_postCode=&user_warehouseId=0&variants=&warehouses=",
}

OUTPUT_COLS = [
    "id","name","url","category_breadcrumb",
    "price_number","price_original","discountPercentage",
    "mediaURL_image","ratingAverage",
    "shop_id","shop_name", "shop_url","shop_city","shop_tier",
    "countSold","isTopAds","labelGroups", "label_titles",
    "totalRating","countReview",
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

    # Banyak kasus: backend paging pakai start (offset), page tetap 1
    d["page"] = "1"

    rows = int(d.get("rows", "60") or "60")

    # start: pakai offset dari server bila ada, kalau belum ada pakai hitungan sendiri
    if carry and carry.get("next_offset_organic"):
        d["start"] = str(carry["next_offset_organic"])
    else:
        d["start"] = str((page - 1) * rows)

    # kirim search_id hanya kalau ada (biasanya dari additionalParams)
    if carry.get("search_id"):
        d["search_id"] = str(carry["search_id"])
    else:
        d.pop("search_id", None)  # pastikan page 1 tidak bawa search_id lama

    return urlencode(d, doseq=True)


def parse_count_sold_from_labelgroups(label_groups) -> Optional[int]:
    """
    Parse "10rb+ terjual" -> 10000, "1,5rb terjual" -> 1500, "2jt+ terjual" -> 2000000, dll.
    """
    if not isinstance(label_groups, list):
        return None

    # ambil label yang mengandung "terjual"
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

    # 1,5 -> 1.5
    num_float = float(num_str.replace(",", "."))

    if unit in ("rb", "ribu", "k"):
        return int(num_float * 1_000)
    if unit in ("jt", "juta", "m"):
        return int(num_float * 1_000_000)

    # tanpa unit: coba treat sebagai angka biasa / ribuan
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
# 3) FETCHERS: SEARCH + REVIEW
# =========================================================

def flatten_search_product(p: Dict[str, Any]) -> Dict[str, Any]:
    label_groups = p.get("labelGroups") or []
    ads = p.get("ads") or {}

    return {
        "id": p.get("id"),
        "name": p.get("name"),
        "url": p.get("url"),
        "category_breadcrumb": (p.get("category") or {}).get("breadcrumb"),
        "price_number": ((p.get("price") or {}).get("number")),
        "price_original": ((p.get("price") or {}).get("original")), 
        "discountPercentage": ((p.get("price") or {}).get("discountPercentage")),
        "mediaURL_image": ((p.get("mediaURL") or {}).get("image")),
        "ratingAverage": float(p.get("rating")) if p.get("rating") not in (None, "") else None,
        # "ratingAverage": float(p["rating"]) if p.get("rating") not in (None, "") else None,
        "shop_id": ((p.get("shop") or {}).get("id")),
        "shop_name": ((p.get("shop") or {}).get("name")),
        "shop_url": ((p.get("shop") or {}).get("url")),
        "shop_city": ((p.get("shop") or {}).get("city")),
        "shop_tier": ((p.get("shop") or {}).get("tier")),

        # tambahan:
        "countSold": parse_count_sold_from_labelgroups(label_groups),
        "isTopAds": is_topads_from_ads(ads),
        "labelGroups": json.dumps(label_groups, ensure_ascii=False),
        "label_titles": " | ".join(
            lg.get("title", "")
            for lg in label_groups
            if isinstance(lg, dict)
        ),
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
    count_review = rating.get("totalRatingTextAndImage")  # cocok jadi "countReview"
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
    per_run_page_limit: int = 5,                 # ambil N halaman per eksekusi
    out_csv: str = "tokopedia_enriched.csv",
    state_path: str = "state.json",
    page_pause_range: Tuple[float, float] = (8, 20),
):
    print("[START] running scrape...")

    """
    - Ambil search products per halaman
    - Enrich tiap produk dengan totalRating + countReview
    - Tambah countSold + isTopAds dari search response
    - Simpan bertahap (append) + state untuk resume
    """
    session = requests.Session()
    session.get("https://www.tokopedia.com/", headers={"user-agent": SEARCH_CFG["headers"]["user-agent"]}, timeout=30)

    pr = PoliteRequester(
        min_delay=2.0,
        max_delay=5.0,
        long_break_every=60,
        long_break_range=(60, 180),
        max_retries=6,
    )

    state = load_state(state_path)
    start_page = int(state.get("page", 1))
    seen_ids = set(state.get("seen_ids", []))
    carry = state.get("carry", {}) or {}

    # penting: kalau mulai dari page 1, jangan bawa carry lama
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
            print(f"[WARN] carry_next={carry_next}")  # biar kelihatan search_id/offset yang dikasih server
            #print("[WARN] responseCode =", responseCode)
            #print("[WARN] additionalParams =", header.get("additionalParams"))
            #print("[WARN] backendFilters =", header.get("backendFilters"))

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

            try:
                totalRating, countReview, ratingAvgFromPDP = fetch_review_summary(pr, session, pid)
                row["totalRating"] = totalRating
                row["countReview"] = countReview
                if ratingAvgFromPDP is not None:
                    row["ratingAverage"] = ratingAvgFromPDP
            except Exception:
                row["totalRating"] = None
                row["countReview"] = None

            seen_ids.add(pid)
            rows_to_save.append(row)

        append_rows_to_csv(out_csv, rows_to_save)

        # update carry untuk next page & simpan state
        carry = carry_next
        save_state(state_path, page + 1, seen_ids, carry)

        print(f"[SAVE] page {page} appended={len(rows_to_save)} total_seen={len(seen_ids)} / totalData={totalData} -> {out_csv}")

        # stop kalau server bilang hasil habis
        if str(carry.get("has_more", "")).lower() == "false":
            print("[STOP] has_more=false (server bilang hasil sudah habis)")
            break

        # pause antar halaman
        pause = random.uniform(*page_pause_range)
        print(f"[PAUSE] between pages {pause:.1f}s")
        time.sleep(pause)

        pages_done += 1
        if pages_done >= per_run_page_limit:
            print("[DONE] stop bertahap. Jalankan lagi untuk lanjut (resume dari state.json).")
            break


# =========================================================
# CONTOH PAKAI
# =========================================================
# run_scrape_enrich_batched(keyword="..?..", per_run_page_limit=3, out_csv="..?.._enriched.csv")
if __name__ == "__main__":
    # (opsional) bikin folder output
    os.makedirs("output", exist_ok=True)

    run_scrape_enrich_batched(
        keyword="Kain Daerah",                          # ganti kalau mau scraping lagi
        per_run_page_limit=1,                          # ambil 1 halaman (karena setiap run cuma dapat 1 page)
        out_csv="output/kainDaerah_enriched.csv",       # ini juga ganti
        state_path="output/state_kainDaerah.json",      # ini juga ganti
        page_pause_range=(8, 20),
    )
