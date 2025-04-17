import streamlit as st
import polars as pl
import pandas as pd
import logging
from chart_utils import create_themed_histogram

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_block_distribution_section(data):
    """Render block distribution analysis"""
    # Unpack data
    block_stats = data["block_stats"]
    
    st.header("Block Distribution Analysis")
    
    with st.container():
        # Create histograms of propagation times
        try:
            # Convert to pandas for Plotly
            block_stats_pd = block_stats.to_pandas()
            
            # Create histogram for min propagation times
            fig = create_themed_histogram(
                block_stats_pd,
                x="min_propagation_ms",
                nbins=30,
                title="Distribution of Minimum Block Propagation Times",
                xaxis_title="Min Propagation Time (ms)",
                yaxis_title="Block Count"
            )
            
            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            logger.error(f"Error in block distribution analysis: {str(e)}")
            st.error(f"Error creating block distribution charts: {str(e)}")