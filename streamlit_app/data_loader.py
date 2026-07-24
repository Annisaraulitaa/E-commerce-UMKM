import pandas as pd
import streamlit as st
import os
import gdown

from config import DATA_ID, DATA_PATH_LOCAL


@st.cache_data
def load_data():

    if not os.path.exists(DATA_PATH_LOCAL):
        gdown.download(
            id=DATA_ID,
            output=DATA_PATH_LOCAL,
            quiet=False
        )

    df = pd.read_csv(DATA_PATH_LOCAL)

    df["umkm_binary"] = (
        pd.to_numeric(
            df["umkm_label"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    if "image_url" in df.columns:
        df["image_display"] = df["image_url"].fillna("")  # fallback kalau kosong
    else:
        df["image_display"] = df["image_local_path"].fillna("")

    return df