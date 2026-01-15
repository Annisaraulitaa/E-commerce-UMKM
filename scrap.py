import requests
import pandas as pd

url_target = 'https://gql.tokopedia.com/graphql/SearchProductV5Query' 

header = { 
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'x-price-center': 'true',
    'iris_session_id': 'd3d3LnRva29wZWRpYS5jb20=.8d1dadc5ea75446108a3a9d234eed6e1.1754465805078',
    'bd-device-id': '7527216031573837313',
    'sec-ch-ua-mobile': '?0',
    'accept': '*/*',
    'content-type': 'application/json',
    'bd-web-id': '7527216031573837313',
    'x-version': 'fccf089',
    'Referer': 'https://www.tokopedia.com/',
    'x-source': 'tokopedia-lite',
    'x-dark-mode': 'false',
    'tkpd-userid':'0',
    'x-device': 'desktop-0.0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-tkpd-lite-service': 'zeus'
}

query = f'[{{"operationName":"SearchProductV5Query","variables":{{"params":"device=desktop&enter_method=normal_search&l_name=sre&navsource=&ob=23&page=1&q=umkm&related=true&rows=60&safe_search=false&sc=&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=ab63b850c54e964212ffea525b4d8a19&user_addressId=&user_cityId=176&user_districtId=2274&user_id=&user_lat=&user_long=&user_postCode=&user_warehouseId=&variants=&warehouses="}},"query":"query SearchProductV5Query($params: String!) {{ searchProductV5(params: $params) {{ data {{ products {{ id name url mediaURL {{ image }} shop {{ name city }} price {{ text number }} rating category {{ name breadcrumb }} labelGroups {{ title }} }} }} }} }}"}}]'

# Kirim request
response = requests.post(url_target, headers=header, data=query)
products = response.json()[0]['data']['searchProductV5']['data']['products']

print(f"Jumlah produk: {len(products)}")

# Ubah ke DataFrame dengan kolom penting saja
df = pd.DataFrame([{
    "id": p.get("id"),
    "nama": p.get("name"),
    "harga_text": p.get("price", {}).get("text"),
    "harga_number": p.get("price", {}).get("number"),
    "rating": p.get("rating"),
    "kategori": p.get("category", {}).get("name"),
    "breadcrumb": p.get("category", {}).get("breadcrumb"),
    "toko": p.get("shop", {}).get("name"),
    "kota": p.get("shop", {}).get("city"),
    "gambar": p.get("mediaURL", {}).get("image"),
    "url": p.get("url")
} for p in products])

# Simpan ke CSV
df.to_csv("tokopedia_umkm.csv", index=False, encoding="utf-8-sig")
print("✅ Data disimpan ke tokopedia_umkm.csv")
