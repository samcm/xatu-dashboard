import streamlit as st
import polars as pl
from datetime import datetime, timedelta
import logging
from utils import load_xatu_data, load_xatu_data_range
from config import SUPPORTED_NETWORKS, TIME_WINDOWS, DEFAULT_REFRESH_TIME
from dashboards.block_arrival.data_processing import preprocess_data
from dashboards.block_arrival.sections import (
    render_summary_section,
    render_block_distribution_section, 
    render_client_analysis_section,
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
    
    # Get global settings from session state
    network = st.session_state.network
    time_window = st.session_state.time_window
    force_refresh = force_refresh or st.session_state.force_refresh
    
    # Calculate date range based on the selected time window
    days = TIME_WINDOWS[time_window]
    end_date = datetime.now().date() - timedelta(days=3)  # To avoid data availability issues
    start_date = end_date - timedelta(days=days-1)
    
    # Show progress while loading data
    with st.spinner(f"Loading {network} data for the past {days} days..."):
        try:
            # Show a progress bar
            progress_bar = st.progress(0)
            
            # Define a callback function to update the progress bar
            def update_progress(progress):
                progress_bar.progress(progress)
            
            # Load data for the selected date range
            if days == 1:
                # Use the existing single-day function for backward compatibility
                raw_df = load_xatu_data(network, "beacon_api_eth_v1_events_block", end_date, use_cache=not force_refresh)
            else:
                # Use the new range function for multiple days
                raw_df = load_xatu_data_range(
                    network,
                    "beacon_api_eth_v1_events_block",
                    start_date,
                    end_date,
                    use_cache=not force_refresh,
                    progress_callback=update_progress
                )
            
            # Clear the progress bar after loading
            progress_bar.empty()
            
            if raw_df is None or len(raw_df) == 0:
                st.warning(f"No data available for the selected time period ({start_date.isoformat()} to {end_date.isoformat()})")
                return
                
            # Display the data size
            st.info(f"Loaded {len(raw_df):,} observations from {start_date.isoformat()} to {end_date.isoformat()}")
                
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
            
    except Exception as e:
        st.error(f"Error displaying results: {str(e)}")
        st.exception(e)
        return
    
    # Meta information
    st.markdown(f"*Data from {start_date.isoformat()} to {end_date.isoformat()}. Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

if __name__ == "__main__":
    render() 