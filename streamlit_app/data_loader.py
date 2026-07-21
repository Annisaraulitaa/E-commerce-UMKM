import pandas as pd
import streamlit as st

from config import DATA_PATH


@st.cache_data
def load_data():

    df = pd.read_csv(DATA_PATH)

    df["umkm_binary"] = (
        pd.to_numeric(
            df["umkm_label"],
            errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )

    return df