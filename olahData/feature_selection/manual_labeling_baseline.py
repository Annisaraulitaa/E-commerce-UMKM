from pathlib import Path
import pandas as pd

def extract_top_k_for_labeling(input_csv, output_csv, top_k=30):
    """
    Ambil Top-K per query dari output BM25 candidate pool untuk manual labeling
    """
    df = pd.read_csv(input_csv)
    
    # Pastikan kolom query dan rank_bm25 ada
    if 'query' not in df.columns or 'rank_bm25' not in df.columns:
        raise ValueError("Kolom 'query' dan/atau 'rank_bm25' tidak ada di input CSV.")
    
    # Urutkan per query
    df = df.sort_values(['query', 'rank_bm25'])
    
    # Ambil Top-K per query
    top_k_df = df.groupby('query', group_keys=False).head(top_k).copy()
    
    # Tambahkan kolom kosong untuk manual labeling
    top_k_df['manual_label'] = ''
    top_k_df['label_note'] = ''
    
    # Simpan ke CSV
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    top_k_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"File Top-{top_k} per query siap dilabeli manual: {output_path}")
    print(f"Total query: {top_k_df['query'].nunique()}, total baris: {len(top_k_df):,}")

# Contoh penggunaan
if __name__ == "__main__":
    extract_top_k_for_labeling(
        input_csv="output_bm25_candidates/bm25_candidates_all_queries.csv",
        output_csv="manual_labeling_top30.csv",
        top_k=30  # bisa diubah sesuai kebutuhan
    )