import requests
import json

url = 'https://gql.tokopedia.com/graphql/SearchProductQuery'

headers = {
    "accept": "*/*",
    "bd-device-id": "7527216031573837313",
    "content-type": "application/json",
    "iris_session_id": "",
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "tkpd-userid": "0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-device": "desktop-0.0",
    "x-price-center": "true",
    "x-source": "tokopedia-lite",
    "x-tkpd-lite-service": "zeus"
}


payload = {
    "operationName": "HomeRecommendationCardQuery",
    "variables": {
        "bytedanceSessionID": "",
        "layouts": "product,banner_ads",
        "location": "user_addressId=&user_cityId=176&user_districtId=2274&user_lat=0&user_long=0&user_postCode=&warehouse_ids=",
        "param": "id=381&sourceType=manual_page:homepage--&name=For You&position=1",
        "productCardVersion": "v5",
        "productPage": 1,
        "refreshType": 0
    },
    "query": "query HomeRecommendationCardQuery($productPage: Int, $param: String, $layouts: String, $location: String, $productCardVersion: String, $refreshType: Int, $bytedanceSessionID: String) {\n  getHomeRecommendationCard(productPage: $productPage, param: $param, layouts: $layouts, location: $location, productCardVersion: $productCardVersion, refreshType: $refreshType, bytedanceSessionID: $bytedanceSessionID) {\n    pageName\n    layoutName\n    hasNextPage\n    appLog {\n      bytedanceSessionID\n      requestID\n      logID\n      __typename\n    }\n    cards {\n      oldId: id\n      id: id_str_auto_\n      layout\n      layoutTracker\n      dataStringJson\n      url\n      name\n      subtitle\n      price\n      rating\n      applink\n      clickUrl\n      imageUrl\n      iconUrl\n      isTopads\n      priceInt\n      clusterID\n      productKey\n      isWishlist\n      wishlistUrl\n      countReview\n      slashedPrice\n      ratingAverage\n      trackerImageUrl\n      slashedPriceInt\n      discountPercentage\n      recommendationType\n      categoryBreadcrumbs\n      gradientColor\n      oldCategoryID: categoryID\n      categoryID: categoryID_str_auto_\n      oldWarehouseID: warehouseID\n      warehouseID: warehouseID_str_auto_\n      label {\n        imageUrl\n        title\n        backColor\n        textColor\n        __typename\n      }\n      shop {\n        oldId: id\n        id: id_str_auto_\n        url\n        city\n        name\n        domain\n        applink\n        imageUrl\n        reputation\n        __typename\n      }\n      badges {\n        title\n        imageUrl\n        __typename\n      }\n      freeOngkir {\n        isActive\n        imageUrl\n        __typename\n      }\n      labelGroup {\n        url\n        type\n        title\n        position\n        styles {\n          key\n          value\n          __typename\n        }\n        __typename\n      }\n      recParam\n      oldParentProductID: parentProductID\n      parentProductID: parentProductID_str_auto_\n      countSold\n      slotID\n      creativeID\n      logExtra\n      dynamicTracker\n      __typename\n    }\n    __typename\n  }\n}\n"
}


response = requests.post(url, headers=headers, json=payload)

data = response.json()["data"]["getHomeRecommendationCard"]["cards"][0]
# data = response.json()["data"]["getHomeRecommendationCard"]["cards"]

print(json.dumps(data, indent=2, ensure_ascii=False))
# for product in data:
#     print(product)
