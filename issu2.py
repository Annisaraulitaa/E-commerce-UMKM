import requests
import json
import time
import random

# URL untuk GraphQL
url = 'https://gql.tokopedia.com/graphql/HomeRecommendationCardQuery'

# Headers untuk request
headers = {
    "accept": "*/*",
    "bd-device-id": "7527216031573837313",
    "content-type": "application/json",
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "tkpd-sessionid": "nzGmJLVXwWB8fGvlAFF2A1V+nU3daQ5z+fT86T6HOdc=",
    "tkpd-userid": "0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-device": "desktop-0.0",
    "x-price-center": "true",
    "x-source": "tokopedia-lite",
    "x-tkpd-lite-service": "zeus",
    "x-version": "3a8a36b"
}


# Payload untuk GraphQL
payload = {
    "operationName": "HomeRecommendationCardQuery",
    "variables": {
        "bytedanceSessionID": "",
        "layouts": "product,banner_ads",
        "location": "user_addressId=&user_cityId=176&user_districtId=2274&user_lat=0&user_long=0&user_postCode=&warehouse_ids=",
        "param": "id=381&sourceType=manual_page:homepage--&name=For You&position=1",
        "productCardVersion": "v5",
        "productPage": 1,
        "refreshType": 0,
        "cursor": 0,
        "exposureItems": ""
    },
    "query": "query HomeRecommendationCardQuery($productPage: Int, $param: String, $layouts: String, $location: String, $productCardVersion: String, $refreshType: Int, $bytedanceSessionID: String, $exposureItems: String, $cursor: Int) {\n  getHomeRecommendationCard(productPage: $productPage, param: $param, layouts: $layouts, location: $location, productCardVersion: $productCardVersion, refreshType: $refreshType, bytedanceSessionID: $bytedanceSessionID, exposureItems: $exposureItems, cursor: $cursor) {\n    cursor\n    pageName\n    layoutName\n    hasNextPage\n    appLog {\n      bytedanceSessionID\n      requestID\n      logID\n      __typename\n    }\n    cards {\n      oldId: id\n      id: id_str_auto_\n      layout\n      layoutTracker\n      dataStringJson\n      url\n      name\n      subtitle\n      price\n      rating\n      applink\n      clickUrl\n      imageUrl\n      iconUrl\n      isTopads\n      priceInt\n      clusterID\n      productKey\n      isWishlist\n      wishlistUrl\n      countReview\n      slashedPrice\n      ratingAverage\n      trackerImageUrl\n      slashedPriceInt\n      discountPercentage\n      recommendationType\n      categoryBreadcrumbs\n      gradientColor\n      oldCategoryID: categoryID\n      categoryID: categoryID_str_auto_\n      oldWarehouseID: warehouseID\n      warehouseID: warehouseID_str_auto_\n      label {\n        imageUrl\n        title\n        backColor\n        textColor\n        __typename\n      }\n      shop {\n        oldId: id\n        id: id_str_auto_\n        url\n        city\n        name\n        domain\n        applink\n        imageUrl\n        reputation\n        __typename\n      }\n      badges {\n        title\n        imageUrl\n        __typename\n      }\n      freeOngkir {\n        isActive\n        imageUrl\n        __typename\n      }\n      labelGroup {\n        url\n        type\n        title\n        position\n        styles {\n          key\n          value\n          __typename\n        }\n        __typename\n      }\n      recParam\n      oldParentProductID: parentProductID\n      parentProductID: parentProductID_str_auto_\n      countSold\n      slotID\n      creativeID\n      logExtra\n      dynamicTracker\n      __typename\n    }\n    __typename\n  }\n}\n"
}

# Fungsi untuk mengambil data produk
def fetch_data(page=1):
    payload["variables"]["params"] = f"&page={page}&ob=23&identifier=fashion-pria_atasan-pria&sc=1784&user_id=0&rows=60&start={page*60}&source=directory&device=desktop&related=true&st=product&safe_search=false"
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        try:
            response_data = response.json()  # Mengambil respons JSON
            print(f"Response for page {page}:")
            print(json.dumps(response_data, indent=2, ensure_ascii=False))  # Mencetak respons JSON untuk debug
            
            # Mengambil data dari response
            data = response_data.get("data", {}).get("CategoryProducts", {}).get("data", [])
            if data:
                return data
            else:
                print(f"No product data found for page {page}.")
                return []
        except (KeyError, TypeError) as e:
            print(f"Error parsing data for page {page}: {e}")
            return []
    else:
        print(f"Request failed for page {page} with status code {response.status_code}")
        return []

# Ambil data dari halaman 1 hingga 24
all_data = []
for page in range(1, 25):  # Halaman 1 sampai 24
    print(f"Fetching data for page {page}...")
    page_data = fetch_data(page)
    if page_data:
        all_data.extend(page_data)  # Menambahkan data ke list
    time.sleep(random.uniform(2, 5))  # Delay acak untuk menghindari pemblokiran

# Simpan data ke dalam file JSON
with open('products_data.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)

print(f"Data fetched and saved for {len(all_data)} products.")
