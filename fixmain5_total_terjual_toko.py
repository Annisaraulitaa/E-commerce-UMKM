# ============================================================
# REFRESH SHOP PUBLIC SALES FOR MISSING ShopInfoCore DATA
# ============================================================
# Tujuan:
# - Mengambil hanya toko dengan shopinfo_product_sold == 0
# - Membuka halaman toko Tokopedia
# - Mengambil angka "xxx terjual" dari halaman toko
# - Menyimpan hasil ke CSV baru level toko
# - Tidak mengubah dataset utama
# ============================================================

import os
import re
import csv
import json
import time
import random
from typing import Any, Dict, List

import pandas as pd

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================
# CONFIG
# ============================================================

INPUT_CSV = r"D:\Kuliah\DATA_TA\olahData\preprocessing\nondup_dataset.csv"
OUTPUT_CSV = r"output4/shop_missing_atribut_refresh.csv"
CHECKPOINT = r"output4/shop_missing_atribut_checkpoint.json"

# Test awal
LIMIT = 5

HEADLESS = False

SAVE_EVERY = 25

PAGE_TIMEOUT = 40000
WAIT_RANGE = (2, 4)

MIN_DELAY = 4
MAX_DELAY = 8
LONG_BREAK_EVERY = 25
LONG_BREAK_RANGE = (90, 240)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)


# ============================================================
# HELPERS
# ============================================================

def safe_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except:
        pass
    return str(x).strip()


def extract_shop_domain(url):
    """Ambil URL toko dari shop_url/product_url."""
    url = safe_str(url)
    if not url:
        return ""

    if not url.startswith("http"):
        url = "https://" + url

    parts = url.split("/")

    # https://www.tokopedia.com/nama-toko/produk
    try:
        return f"https://www.tokopedia.com/{parts[3]}"
    except:
        return ""


def parse_sold_number(text):
    """Contoh:
    '118 rb terjual' -> 118000
    '6 jt terjual' -> 6000000
    """
    if not text:
        return 0

    text = text.lower().replace("+", "")

    match = re.search(
        r"([0-9.,]+)\s*(rb|ribu|jt|juta|k|m)?",
        text
    )

    if not match:
        return 0

    number = float(match.group(1).replace(",", "."))
    unit = match.group(2)

    if unit in ["rb", "ribu", "k"]:
        number *= 1000
    elif unit in ["jt", "juta", "m"]:
        number *= 1000000

    return int(number)


def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, "r") as f:
            return json.load(f)
    return {"index": 0}


def save_checkpoint(index):
    with open(CHECKPOINT, "w") as f:
        json.dump({"index": index}, f)


# ============================================================
# EKSTRAK DATA DARI HALAMAN TOKO
# ============================================================

def extract_shop_sales(page):

    result = {
        "shop_public_sold_text": "",
        "shop_public_sold": 0,
        "shop_public_rating": None,
        "shop_public_review_count": None,
        "shop_sales_source": "shopRatingDetailHeader"
    }

    try:

        # Ambil komponen rating toko langsung dari DOM
        container = page.locator(
            '[data-testid="shopRatingDetailHeader"]'
        )


        if container.count() == 0:
            result["error"] = "shopRatingDetailHeader tidak ditemukan"
            return result


        # ==========================
        # Rating
        # contoh:
        # <b>4.8</b>
        # ==========================

        bold_elements = container.locator("b")

        if bold_elements.count() >= 1:

            rating_text = bold_elements.nth(0).inner_text()

            result["shop_public_rating"] = float(
                rating_text.strip()
            )


        # ==========================
        # Review count
        # contoh:
        # (1.179)
        # ==========================

        spans = container.locator("span")

        if spans.count() >= 2:

            review_text = spans.nth(1).inner_text()

            review_text = (
                review_text
                .replace("(", "")
                .replace(")", "")
                .replace(".", "")
                .replace(",", "")
                .strip()
            )

            if review_text:
                result["shop_public_review_count"] = int(
                    review_text
                )


        # ==========================
        # Total terjual
        # contoh:
        # <b>27 rb</b>
        # ==========================

        if bold_elements.count() >= 2:

            sold_text = bold_elements.nth(1).inner_text()

            result["shop_public_sold_text"] = sold_text

            result["shop_public_sold"] = parse_sold_number(
                sold_text
            )


    except Exception as e:

        result["error"] = str(e)


    return result


# ============================================================
# MAIN
# ============================================================

def main():

    df = pd.read_csv(INPUT_CSV)

    # Ambil hanya toko dengan product sold ShopInfoCore = 0
    missing = df[
        pd.to_numeric(
            df["shopinfo_product_sold"],
            errors="coerce"
        ).fillna(0) == 0
    ]

    shops = (
        missing[
            [
                "shop_id",
                "shop_name",
                "shop_url"
            ]
        ]
        .drop_duplicates("shop_id")
        .reset_index(drop=True)
    )

    print("Total toko perlu refresh:", len(shops))

    checkpoint = load_checkpoint()
    start = checkpoint["index"]

    if LIMIT:
        end = min(start + LIMIT, len(shops))
    else:
        end = len(shops)

    print(
        f"Processing toko {start} sampai {end-1}"
    )

    browser_results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS
        )

        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="id-ID"
        )

        page = context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT)

        for i in range(start, end):

            row = shops.iloc[i]

            shop_url = safe_str(row["shop_url"])

            if not shop_url:
                shop_url = extract_shop_domain(
                    row["shop_name"]
                )

            print(
                f"[{i}] {row['shop_name']}"
            )

            result = {
                "shop_id": row["shop_id"],
                "shop_name": row["shop_name"],
                "shop_url": shop_url,
                "status": "error"
            }

            try:
                page.goto(
                    shop_url,
                    wait_until="domcontentloaded",
                    timeout=PAGE_TIMEOUT
                )

                time.sleep(
                    random.uniform(*WAIT_RANGE)
                )

                result.update(
                    extract_shop_sales(page)
                )

                result["status"] = "success"

                print(
                    " sold:",
                    result["shop_public_sold"]
                )

            except PlaywrightTimeoutError:
                result["status"] = "timeout"

            except Exception as e:
                result["error"] = str(e)

            browser_results.append(result)

            if len(browser_results) >= SAVE_EVERY:
                save_batch(browser_results)
                browser_results.clear()
                save_checkpoint(i+1)

            time.sleep(
                random.uniform(MIN_DELAY, MAX_DELAY)
            )

            if LONG_BREAK_EVERY and (i+1) % LONG_BREAK_EVERY == 0:
                pause = random.uniform(*LONG_BREAK_RANGE)
                print("LONG BREAK", pause)
                time.sleep(pause)

        browser.close()

    if browser_results:
        save_batch(browser_results)

    save_checkpoint(end)

    print("SELESAI")



def save_batch(rows):
    if not rows:
        return

    exists = os.path.exists(OUTPUT_CSV)

    pd.DataFrame(rows).to_csv(
        OUTPUT_CSV,
        mode="a",
        header=not exists,
        index=False,
        encoding="utf-8-sig"
    )


if __name__ == "__main__":
    main()
