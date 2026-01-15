import requests, time, json
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from datetime import datetime

url = 'https://gql.tokopedia.com/graphql/DiscoveryComponentQuery'

headers = {
    "accept": "*/*",
    "bd-device-id": "7527216031573837313",
    "content-type": "application/json",
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "x-price-center": "true",
    "x-source": "tokopedia-lite",
    "x-tkpd-lite-service": "zeus",
    "x-version": "f4c0f0a"
}

all_products = []

# --- ubah range yang ingin difetch sesuai kebutuhan ---
for page in range(1, 100):
    print(f"Fetching page {page}...")
    # payload = {
    #     "operationName": "HomeRecommendationCardQuery",
    #     "variables": {
    #         "bytedanceSessionID": "",
    #         "layouts": "product,banner_ads",
    #         "location": "user_addressId=&user_cityId=176&user_districtId=2274&user_lat=0&user_long=0&user_postCode=&warehouse_ids=",
    #         "param": "id=381&sourceType=manual_page:homepage--&name=For You&position=1",
    #         "productCardVersion": "v5",
    #         "productPage": page,
    #         "refreshType": 0
    #     },
    #     "query": "query HomeRecommendationCardQuery($productPage: Int, $param: String, $layouts: String, $location: String, $productCardVersion: String, $refreshType: Int, $bytedanceSessionID: String) {\n  getHomeRecommendationCard(productPage: $productPage, param: $param, layouts: $layouts, location: $location, productCardVersion: $productCardVersion, refreshType: $refreshType, bytedanceSessionID: $bytedanceSessionID) {\n    pageName\n    layoutName\n    hasNextPage\n    cards {\n      oldId: id\n      id: id_str_auto_\n      layout\n      layoutTracker\n      dataStringJson\n      url\n      name\n      subtitle\n      price\n      rating\n      applink\n      clickUrl\n      imageUrl\n      iconUrl\n      isTopads\n      priceInt\n      clusterID\n      productKey\n      isWishlist\n      wishlistUrl\n      countReview\n      slashedPrice\n      ratingAverage\n      trackerImageUrl\n      slashedPriceInt\n      discountPercentage\n      recommendationType\n      categoryBreadcrumbs\n      gradientColor\n      oldCategoryID: categoryID\n      categoryID: categoryID_str_auto_\n      oldWarehouseID: warehouseID\n      warehouseID: warehouseID_str_auto_\n      label { imageUrl title backColor textColor __typename }\n      shop { oldId: id id: id_str_auto_ url city name domain applink imageUrl reputation __typename }\n      badges { title imageUrl __typename }\n      freeOngkir { isActive imageUrl __typename }\n      labelGroup { url type title position styles { key value __typename } __typename }\n      recParam\n      oldParentProductID: parentProductID\n      parentProductID: parentProductID_str_auto_\n      countSold\n      slotID\n      creativeID\n      logExtra\n      dynamicTracker\n      __typename\n    }\n    __typename\n  }\n}\n"

    payload = {
        "operationName": "DiscoveryComponentQuery",
        "variables": {
            "device": "desktop",
            "identifier": "clp_rumah-tangga_984",
            "componentId": "62",
            "filters": "{\"rpc_page_number\":\"2\",\"rpc_page_size\":\"10\",\"rpc_ProductId\":\"\",\"l_name\":\"sre\",\"rpc_next_page\":\"p\",\"rpc_UserCityId\":\"176\",\"rpc_UserDistrictId\":\"2274\"}",
            "exposure_items": "H4sIAAAAAAAAA4uOBQApu0wNAgAAAA",
            "refresh_type": "2",
            "current_session_id": "r3:20251206150521FF74E98B8F6F700F4DSF",
            "cursor": 0
        },
        "query": "query DiscoveryComponentQuery($identifier: String!, $componentId: String!, $filters: String, $device: String = \"desktop\", $exposure_items: String, $refresh_type: String, $current_session_id: String, $cursor: Int) {\n  componentInfo(identifier: $identifier, component_id: $componentId, filters: $filters, device: $device, exposure_items: $exposure_items, refresh_type: $refresh_type, current_session_id: $current_session_id, cursor: $cursor) {\n    data\n    __typename\n  }\n}\n"
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()

    cards = resp.json()["data"]["getHomeRecommendationCard"]["cards"] or []

    print(f"  → {len(cards)} produk")

    all_products.extend(cards)
    time.sleep(0.8)

print(f"Total produk diambil: {len(all_products)}")

# --- FLATTEN semua key nested pakai dot ---
df = json_normalize(all_products, sep='.')

# --- ubah kolom bertipe list/dict menjadi JSON string supaya aman untuk CSV/XLSX ---
for col in df.columns:
    sample = df[col].dropna().head(1)
    if len(sample) and isinstance(sample.iloc[0], (list, dict)):
        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

# --- Menghilangkan nilai yang duplikat berdasarkan 'id' kalau ada (nama file harus sama kalau ingin code ini bekerja) ---
# if 'id' in df.columns:
#     before = len(df)
#     df = df.drop_duplicates(subset=['id'])
#     print(f"Dedup by id: {before} → {len(df)}")

# --- Save ke lokal ---
drive_dir = Path("./data")
drive_dir.mkdir(parents=True, exist_ok=True)

ts = datetime.now().strftime("%Y%m%d-%H%M%S")
csv_path = (drive_dir / f"tokped-{ts}.csv").resolve()

# CSV
df.to_csv(csv_path, index=False, encoding="utf-8")

print(f"✅ Disimpan CSV: {csv_path}  ({len(df)} baris, {df.shape[1]} kolom)")
