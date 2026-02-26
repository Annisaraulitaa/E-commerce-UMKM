"""Copy and reorder CSVs ending with _with_images.csv

Usage:
  python scripts/copy_and_reorder_with_images.py 
  python scripts/copy_and_reorder_with_images.py --src "data lama/ulang lagi 2" --dst output --recursive

The script tries to import `OUTPUT_COLS` from `fixmain2.py`. If import fails, it falls back to a built-in list.
"""
from pathlib import Path
import argparse
import csv
import sys
from typing import Optional

try:
    import pandas as pd
    _HAS_PANDAS = True
except Exception:
    _HAS_PANDAS = False

# try to get OUTPUT_COLS from fixmain2
try:
    import fixmain2
    OUTPUT_COLS = getattr(fixmain2, "OUTPUT_COLS")
except Exception:
    OUTPUT_COLS = [
        "id","name","url","category_breadcrumb",
        "price_number","price_original","discountPercentage",
        "mediaURL_image","mediaURL_image_fixed",
        "ratingAverage",
        "shop_id","shop_name","shop_url","shop_city","shop_tier",
        "countSold","isTopAds","labelGroups", "label_titles",
        "totalRating","countReview",
        "image_status","image_source","image_local_path",
    ]

    def _derive_images_folder_name(stem: str) -> str:
            """Derive images folder name from CSV stem.

            Examples:
                'celanaPria_enriched_with_images' -> base 'celanaPria_enriched' -> 'celanaPria_enriched_images'
                'foo_with_images' -> base 'foo' -> 'foo_enriched_images'
            """
            base = stem.replace("_with_images", "")
            if base.endswith("_enriched"):
                    return base + "_images"
            return base + "_enriched_images"

def reorder_with_pandas(src: Path, dst: Path):
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    front = [c for c in OUTPUT_COLS if c in cols]
    rest = [c for c in cols if c not in OUTPUT_COLS]
    new_order = front + rest
    df = df.loc[:, new_order]

    # adjust image_local_path to point under dst images folder (do not copy files)
    images_folder = _derive_images_folder_name(src.stem)
    def _map_path(p: Optional[str]) -> str:
        if not p:
            return ""
        name = Path(p).name
        return str(Path(dst.parent) / images_folder / name)

    if "image_local_path" in df.columns:
        df["image_local_path"] = df["image_local_path"].apply(_map_path)

    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)

def reorder_with_csv(src: Path, dst: Path):
    with src.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        cols = reader.fieldnames or []
    front = [c for c in OUTPUT_COLS if c in cols]
    rest = [c for c in cols if c not in OUTPUT_COLS]
    new_order = front + rest
    images_folder = _derive_images_folder_name(src.stem)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=new_order)
        writer.writeheader()
        for r in rows:
            # ensure all keys exist
            out = {k: r.get(k, "") for k in new_order}
            if out.get("image_local_path"):
                name = Path(out["image_local_path"]).name
                out["image_local_path"] = str(Path(dst.parent) / images_folder / name)
            writer.writerow(out)

def process_file(p: Path, dst_dir: Path):
    # destination filename: remove suffix _with_images from filename
    new_name = p.name.replace("_with_images", "")
    dst = dst_dir / new_name
    if _HAS_PANDAS:
        reorder_with_pandas(p, dst)
    else:
        reorder_with_csv(p, dst)
    print(f"WROTE: {dst}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data lama/ulang lagi 2", help="Source folder to search")
    ap.add_argument("--dst", default="output", help="Destination folder to copy reordered files")
    ap.add_argument("--recursive", action="store_true", help="Search recursively")
    ap.add_argument("--pattern", default="*_with_images.csv", help="Filename glob pattern")
    args = ap.parse_args()

    src_dir = Path(args.src)
    dst_dir = Path(args.dst)
    if not src_dir.exists():
        print(f"Source folder not found: {src_dir}")
        sys.exit(1)

    if args.recursive:
        files = list(src_dir.rglob(args.pattern))
    else:
        files = list(src_dir.glob(args.pattern))

    if not files:
        print("No matching files found")
        return

    for p in files:
        try:
            process_file(p, dst_dir)
        except Exception as e:
            print(f"FAILED {p}: {e}")

if __name__ == "__main__":
    main()
