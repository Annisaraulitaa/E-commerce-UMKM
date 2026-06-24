import streamlit as st


def render_metric_mini(label, value):
    st.markdown(
        f"""
        <div class="metric-mini">
            <div class="metric-mini-label">{label}</div>
            <div class="metric-mini-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )