import streamlit as st
import polars as pl
import requests
from pathlib import Path
from datetime import datetime, timedelta
import altair as alt
from config import SUPPORTED_NETWORKS, TIME_WINDOWS, DEFAULT_REFRESH_TIME

# Import dashboard modules
from dashboards.block_arrival import render as render_block_arrival

# Set page config
st.set_page_config(page_title="Xatu Dashboard", layout="wide")

# Sidebar with dashboard selection
st.sidebar.title("Xatu Dashboards")
page = st.sidebar.radio("Select Dashboard", ["Home", "Block Arrival Times"])

# Add force refresh option in sidebar (for development)
st.sidebar.markdown("---")
st.sidebar.subheader("Settings")

# Force refresh option
force_refresh = st.sidebar.checkbox("Force refresh data", value=False)

# Time window selection
time_window = st.sidebar.selectbox("Time window", TIME_WINDOWS, index=0)

# Render the selected dashboard
if page == "Home":
    st.title("Xatu Dashboard")
    st.write("Welcome to the Xatu Dashboard! Select a dashboard from the sidebar.")
    
    # Display dashboard cards
    st.subheader("Available Dashboards")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Block Arrival Times")
        st.markdown("Analyze block propagation times across different networks")
        if st.button("View Block Arrival Dashboard", key="btn_block_arrival"):
            page = "Block Arrival Times"
    
elif page == "Block Arrival Times":
    render_block_arrival(force_refresh=force_refresh) 