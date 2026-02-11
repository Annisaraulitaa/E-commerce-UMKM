import os
import json
import time
import random
import re
import requests
import pandas as pd
import sys
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode

# Fix Unicode encoding for Windows console
sys.stdout.reconfigure(encoding='utf-8')

# =========================================================
# CFG FROM fixmain2.py
# =========================================================

BD_DEVICE_ID = "7527216031573837313"

IMG_HEADERS_CDN = {
    "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "image",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}

PAGE_HEADERS_HTML = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.tokopedia.com/",
    "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}

# =========================================================
# IMAGE HELPERS FROM fixmain2.py
# =========================================================

IMG_RE = re.compile(r"https://[^\"'<> ]+tokopedia-static\.net[^\"'<> ]+\.(?:jpg|jpeg|png|webp)[^\"'<> ]*")
UI_BAD_KEYWORDS = ["assets-tokopedia-lite", "icon", "logo", "sprite", "favicon", "/prod/", "/assets/", "/static/"]

def fix_img_url(u: str) -> str:
    if not isinstance(u, str):
        return ""
    u = u.replace("\\u0026", "&")
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    return u

def is_ui_asset(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in UI_BAD_KEYWORDS)

def score_product_like(url: str) -> int:
    u = url.lower()
    score = 0
    if "/img/" in u: score += 5
    if "tos-alisg" in u: score += 3
    if "aphluv4xwc" in u: score += 2
    if "~tplv-" in u: score += 2
    if is_ui_asset(u): score -= 100
    return score

def extract_img_candidates(html: str) -> List[str]:
    urls = IMG_RE.findall(html)
    seen, out = set(), []
    for u in urls:
        u = fix_img_url(u)
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    out = [u for u in out if not is_ui_asset(u)]
    out.sort(key=score_product_like, reverse=True)
    return out

def refresh_image_from_product_page(session: requests.Session, product_url: str, page_headers: dict) -> List[str]:
    try:
        r = session.get(product_url, headers=page_headers, timeout=30)
        if r.status_code != 200:
            return []
        candidates = extract_img_candidates(r.text)
        return candidates
    except requests.RequestException:
        return []

def download_image(session: requests.Session, img_url: str, out_path: str, img_headers: dict, min_bytes: int = 20_000) -> Tuple[bool, int]:
    try:
        rr = session.get(img_url, headers=img_headers, timeout=30)
        status = rr.status_code
        ct = rr.headers.get("content-type", "")
        if status == 200 and ct.startswith("image/") and len(rr.content) >= min_bytes:
            with open(out_path, "wb") as f:
                f.write(rr.content)
            return True, status
        return False, status
    except requests.RequestException:
        return False, -1

# =========================================================
# MAIN FUNCTION TO ADD IMAGE COLUMNS TO EXISTING CSV
# =========================================================

def add_image_columns_to_csv(input_csv: str, output_csv: str, img_dir: str, max_products: int = None, max_images_per_product: int = 3):
    """
    Read existing CSV (from fixmain.py), add image columns, scrape and download images,
    then save to new CSV with complete columns like fixmain2.py.
    Resumes from last unprocessed product if output_csv exists.
    """
    os.makedirs(img_dir, exist_ok=True)

    if os.path.exists(output_csv):
        # Resume from existing output CSV
        df = pd.read_csv(output_csv)
        print(f"Resuming from existing CSV: {output_csv} ({len(df)} products)")
    else:
        # Start fresh from input CSV
        df = pd.read_csv(input_csv)
        print(f"Loaded {len(df)} products from {input_csv}")
        # Add new columns for images
        df['mediaURL_image_fixed'] = ""
        df['image_status'] = None
        df['image_source'] = None
        df['image_local_path'] = None

    session = requests.Session()

    processed = 0
    total_images = 0

    for idx, row in df.iterrows():
        if max_products and processed >= max_products:
            break

        try:
            product_id = str(row['id'])
            product_url = row['url']
            product_name = row['name']
            mediaURL_image = row.get('mediaURL_image', '')

            print(f"[{processed+1}] Processing product {product_id}: {product_name[:50]}...")

            # Skip if already processed
            if pd.notna(df.at[idx, 'image_local_path']):
                print("  Already processed, skipping")
                processed += 1
                continue

            # Fix the image URL
            img_fixed = fix_img_url(mediaURL_image) if mediaURL_image else ""
            df.at[idx, 'mediaURL_image_fixed'] = img_fixed

            # Try to download from fixed URL first
            local_path = os.path.join(img_dir, f"{product_id}.jpg")
            image_downloaded = False

            if img_fixed:
                time.sleep(random.uniform(0.3, 0.9))
                try:
                    ok, st = download_image(session, img_fixed, local_path, IMG_HEADERS_CDN)
                    if ok:
                        df.at[idx, 'image_status'] = st
                        df.at[idx, 'image_source'] = "signed"
                        df.at[idx, 'image_local_path'] = local_path
                        image_downloaded = True
                        total_images += 1
                        print(f"  Downloaded from signed URL ({st})")
                    else:
                        print(f"  Failed signed URL ({st})")
                except Exception as e:
                    print(f"  Error downloading signed: {e}")

            # If failed, try refresh from product page
            if not image_downloaded:
                try:
                    candidates = refresh_image_from_product_page(session, product_url, PAGE_HEADERS_HTML)
                    if candidates:
                        # Try first candidate
                        img_url = candidates[0]
                        time.sleep(random.uniform(0.3, 0.9))
                        ok, st = download_image(session, img_url, local_path, IMG_HEADERS_CDN)
                        if ok:
                            df.at[idx, 'image_status'] = st
                            df.at[idx, 'image_source'] = "refreshed"
                            df.at[idx, 'image_local_path'] = local_path
                            image_downloaded = True
                            total_images += 1
                            print(f"  Downloaded from refreshed URL ({st})")
                        else:
                            df.at[idx, 'image_status'] = st
                            print(f"  Failed refreshed URL ({st})")
                    else:
                        print("  No image candidates found")
                except Exception as e:
                    print(f"  Error refreshing: {e}")

            processed += 1

        except Exception as e:
            print(f"  Error processing product {idx}: {e}")
            processed += 1  # Still count as processed to avoid infinite loop
            continue

        # Update CSV after each product for continuous saving
        try:
            df.to_csv(output_csv, index=False)
            print(f"  Updated CSV after processing {processed} products")
        except Exception as e:
            print(f"  Error saving CSV: {e} - Make sure the file is not open in another program (e.g., Excel)")

        # Longer pause between products
        time.sleep(random.uniform(1, 3))

    print(f"\nDone! Processed {processed} products, downloaded {total_images} images.")
    print(f"Final CSV saved to {output_csv}")
    print(f"Output columns: {list(df.columns)}")
    print("Kolom 'name' tersedia untuk join tabel.")

# =========================================================
# USAGE
# =========================================================

if __name__ == "__main__":
    # Input CSV from fixmain.py (without image columns)
    input_csv = r"d:\DATA CACA\00. College\Skripsi\E-commerce-UMKM\data lama\ulang lagi 2\aksesoriAnak_enriched.csv"

    # Output CSV with image columns added
    output_csv = r"d:\DATA CACA\00. College\Skripsi\E-commerce-UMKM\data lama\ulang lagi 2\aksesoriAnak_enriched_with_images.csv"

    # Image directory
    img_dir = r"d:\DATA CACA\00. College\Skripsi\E-commerce-UMKM\data lama\ulang lagi 2\aksesoriAnak_enriched_images"

    # Process all products
    add_image_columns_to_csv(input_csv, output_csv, img_dir, max_products=None, max_images_per_product=1)