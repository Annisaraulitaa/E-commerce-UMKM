import requests, time, json
import pandas as pd
from pandas import json_normalize
from pathlib import Path
from datetime import datetime

url = "https://gql.tokopedia.com/graphql/DiscoveryComponentQuery"

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
    "x-version": "3a8a36b"
}

QUERY = (
    "query DiscoveryComponentQuery($identifier: String!, $componentId: String!, $filters: String, $device: String = \"desktop\", $exposure_items: String, $refresh_type: String, $current_session_id: String, $cursor: Int) {\n  componentInfo(identifier: $identifier, component_id: $componentId, filters: $filters, device: $device, exposure_items: $exposure_items, refresh_type: $refresh_type, current_session_id: $current_session_id, cursor: $cursor) {\n    data\n    __typename\n  }\n}\n"
)

IDENTIFIER = "clp_fashion-pria_1759"
COMPONENT_ID = "88"
CURRENT_SESSION_ID = "r3:202512161401195E1EB7A63A4CA129CV7W"
EXPOSURE_ITEMS = "H4sIAAAAAAAAA4uOBQApu0wNAgAAAA"

PAGE_SIZE = 10
MAX_PAGES = 50
DELAY = 1.0

all_items = []

for page in range(1, MAX_PAGES + 1):
    print(f"\nFetching page {page}...")

    filters_dict = {
        "rpc_page_number": str(page),
        "rpc_page_size": str(PAGE_SIZE),
        "rpc_ProductId": "",
        "l_name": "sre",
        "rpc_next_page": "p",
        "rpc_UserCityId": "176",
        "rpc_UserDistrictId": "2274",
    }

    payload = {
        "operationName": "DiscoveryComponentQuery",
        "variables": {
            "device": "desktop",
            "identifier": IDENTIFIER,
            "componentId": COMPONENT_ID,
            "filters": json.dumps(filters_dict, separators=(",", ":")),
            "exposure_items": EXPOSURE_ITEMS,
            "refresh_type": "2",
            "current_session_id": CURRENT_SESSION_ID,
            "cursor": 0
        },
        "query": QUERY
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=(5, 25))

    # kalau server error/rate limit, jangan crash
    if resp.status_code != 200:
        print(f"  → HTTP {resp.status_code}, skip page ini. Cuplikan: {resp.text[:200]}")
        time.sleep(2)
        continue

    raw = resp.json()
    if isinstance(raw, list):
        raw = raw[0]

    comp_info = (raw.get("data") or {}).get("componentInfo") or {}
    ci_data = comp_info.get("data")

    # kadang ci_data string JSON, kadang object
    if isinstance(ci_data, str):
        try:
            ci_data = json.loads(ci_data)
        except json.JSONDecodeError:
            print("  → Gagal decode componentInfo.data")
            print(str(ci_data)[:300])
            break

    component = (ci_data or {}).get("component") or {}

    # INI target kamu: component.data = [...]
    items = component.get("data") or []

    # info tambahan (opsional)
    add_info = component.get("additional_info") or {}
    total_prod = ((add_info.get("total_product") or {}).get("product_count"))

    print(f"  → {len(items)} item (total_product: {total_prod})")

    if not items:
        print("  → kosong, stop (anggap sudah habis).")
        break

    all_items.extend(items)
    time.sleep(DELAY)

print(f"\nTotal item terkumpul: {len(all_items)}")

if not all_items:
    raise SystemExit("Tidak ada data untuk disimpan.")

# flatten untuk CSV
df = json_normalize(all_items, sep='.')

# ubah list/dict jadi string JSON supaya aman disimpan
for col in df.columns:
    s = df[col].dropna().head(1)
    if len(s) and isinstance(s.iloc[0], (list, dict)):
        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

# simpan
out_dir = Path("./data")
out_dir.mkdir(parents=True, exist_ok=True)

ts = datetime.now().strftime("%Y%m%d-%H%M%S")
csv_path = (out_dir / f"tokped_{ts}.csv").resolve()
df.to_csv(csv_path, index=False, encoding="utf-8")

print(f"✅ Disimpan CSV: {csv_path} ({len(df)} baris, {df.shape[1]} kolom)")
