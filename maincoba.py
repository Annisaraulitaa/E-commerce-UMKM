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

for page in range(1, 10):
    print(f"\nFetching page {page}...")

    # bangun filters pakai dict supaya gampang ganti page
    filters_dict = {
        "rpc_page_number": str(page),       # <-- pakai page di sini
        "rpc_page_size": "10",
        "rpc_ProductId": "",
        "l_name": "sre",
        "rpc_next_page": "p",
        "rpc_UserCityId": "176",
        "rpc_UserDistrictId": "2274"
    }

    payload = {
        "operationName": "DiscoveryComponentQuery",
        "variables": {
            "device": "desktop",
            "identifier": "clp_rumah-tangga_984",
            "componentId": "62",
            "filters": json.dumps(filters_dict, separators=(",", ":")),
            "exposure_items": "H4sIAAAAAAAAA4uOBQApu0wNAgAAAA",
            "refresh_type": "2",
            "current_session_id": "r3:20251206150521FF74E98B8F6F700F4DSF",
            "cursor": 0
        },
        "query": (
            "query DiscoveryComponentQuery($identifier: String!, $componentId: String!, $filters: String, "
            "$device: String = \"desktop\", $exposure_items: String, $refresh_type: String, "
            "$current_session_id: String, $cursor: Int) {"
            "  componentInfo(identifier: $identifier, component_id: $componentId, filters: $filters, "
            "device: $device, exposure_items: $exposure_items, refresh_type: $refresh_type, "
            "current_session_id: $current_session_id, cursor: $cursor) {"
            "    data"
            "    __typename"
            "  }"
            "}"
        )
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
    except requests.RequestException as e:
        print(f"  → Request error di page {page}: {e}")
        continue

    if resp.status_code != 200:
        print(f"  → HTTP {resp.status_code} di page {page}, skip halaman ini.")
        # kalau sering 502/429, bisa time.sleep() lebih lama di sini
        time.sleep(2)
        continue

    try:
        raw = resp.json()
    except ValueError:
        print("  → Gagal parse JSON, cuplikan respons:")
        print(resp.text[:500])
        continue

    # --- ANTISIPASI: kadang respons array [ {...} ], kadang langsung object { ... } ---
    if isinstance(raw, list):
        raw = raw[0]

    # Navigasi aman sampai ke component.data
    root = raw.get("data", {}) or {}
    comp_info = root.get("componentInfo", {}) or {}
    ci_data = comp_info.get("data")

    # Bisa jadi ci_data itu STRING JSON, bisa juga sudah object
    if isinstance(ci_data, str):
        try:
            ci_data = json.loads(ci_data)
        except json.JSONDecodeError:
            print("  → Gagal decode componentInfo.data sebagai JSON string")
            print(ci_data[:300])
            continue

    ci_data = ci_data or {}
    component = ci_data.get("component", {}) or {}
    products = component.get("data", []) or []

    print(f"  → {len(products)} produk")

    # kalau sudah tidak ada produk, anggap page habis, bisa break
    if not products:
        print("  → Tidak ada produk lagi, hentikan loop.")
        break

    all_products.extend(products)
    time.sleep(0.8)

print(f"\nTotal produk diambil: {len(all_products)}")

if not all_products:
    print("❌ Tidak ada data produk yang bisa disimpan, cek lagi respons API-nya.")
else:
    # --- FLATTEN semua key nested pakai dot ---
    df = json_normalize(all_products, sep='.')

    # --- ubah kolom bertipe list/dict menjadi JSON string supaya aman untuk CSV/XLSX ---
    for col in df.columns:
        sample = df[col].dropna().head(1)
        if len(sample) and isinstance(sample.iloc[0], (list, dict)):
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
            )

    # --- Save ke lokal ---
    drive_dir = Path("./data")
    drive_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = (drive_dir / f"tokped-{ts}.csv").resolve()

    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"✅ Disimpan CSV: {csv_path}  ({len(df)} baris, {df.shape[1]} kolom)")
