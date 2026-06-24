import pandas as pd
import streamlit as st

from config import DATA_PATH


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_PATH)

    df["umkm_binary"] = (
        df["umkm_label"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map({"UMKM": 1, "NON_UMKM": 0})
        .fillna(0)
        .astype(int)
    )

    return df