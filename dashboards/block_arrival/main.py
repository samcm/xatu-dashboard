import streamlit as st
import polars as pl
from datetime import datetime, timedelta
import logging
from utils import load_xatu_data
from config import SUPPORTED_NETWORKS, DEFAULT_REFRESH_TIME
from dashboards.block_arrival.data_processing import preprocess_data
from dashboards.block_arrival.sections import (
    render_summary_section,
    render_block_distribution_section, 
    render_client_analysis_section,
    render_hourly_trends_section
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xatu-dashboard")

# Dashboard-specific settings
REFRESH_TIME = DEFAULT_REFRESH_TIME
DASHBOARD_TITLE = "Block Arrival Times"

def render(force_refresh=False):
    """Render the block arrival dashboard"""
    st.title(DASHBOARD_TITLE)
    
    # Single network selection
    network = st.selectbox(
        "Select network", 
        SUPPORTED_NETWORKS,
        index=0
    )
    
    # Calculate date to use (3 days ago to avoid data availability issues)
    date_to_use = datetime.now().date() - timedelta(days=3)
    
    # Show progress while loading data
    with st.spinner(f"Loading {network} data for {date_to_use.isoformat()}..."):
        try:
            raw_df = load_xatu_data(network, "beacon_api_eth_v1_events_block", date_to_use, use_cache=not force_refresh)
            
            if raw_df is None:
                st.warning(f"Data not available for {date_to_use.isoformat()}")
                return
                
            # Preprocess the data
            df = preprocess_data(raw_df, network)
            if df is None:
                st.warning("Error processing data")
                return
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.exception(e)  # Show the full exception
            return
    
    # Render dashboard sections
    try:
        # Summary section
        render_summary_section(df)
        
        # Block distribution section
        render_block_distribution_section(df)
        
        # Client analysis section
        render_client_analysis_section(df)
        
        # Hourly trends section
        render_hourly_trends_section(df, network)
            
    except Exception as e:
        st.error(f"Error displaying results: {str(e)}")
        st.exception(e)
        return
    
    # Meta information
    st.markdown(f"*Data from {date_to_use.isoformat()}. Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

if __name__ == "__main__":
    render() 