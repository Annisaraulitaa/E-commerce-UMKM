import pandas as pd
from heuristic_relevance import is_relevant_product, add_relevance_labels

# Contoh data produk sederhana
data = [
    {
        "name": "Kopi Arabika Gayo Aceh 250gr",
        "category_breadcrumb": "Makanan & Minuman > Kopi",
        "shop_city": "Aceh",
        "shop_name": "UMKM Kopi Gayo",
        "umkm_label": 1
    },
    {
        "name": "Gelas Kopi Keramik Estetik",
        "category_breadcrumb": "Rumah Tangga > Peralatan Minum",
        "shop_city": "Jakarta",
        "shop_name": "Toko Gelas",
        "umkm_label": 0
    },
    {
        "name": "Mesin Kopi Mini Portable",
        "category_breadcrumb": "Elektronik > Mesin Kopi",
        "shop_city": "Bandung",
        "shop_name": "Toko Elektronik",
        "umkm_label": 0
    }
]

df = pd.DataFrame(data)

query = "kopi khas daerah"

# Tambahkan label relevansi
df_labeled = add_relevance_labels(df, query)

print(df_labeled[["name", "category_breadcrumb", "relevance_label"]])