import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
import logging

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_block_distribution_section(data):
    """Render block distribution analysis"""
    # Unpack data
    block_stats = data["block_stats"]
    
    st.header("Block Distribution Analysis")
    
    # Create histograms of propagation times
    try:
        # Convert to pandas for Altair
        block_stats_pd = block_stats.to_pandas()
        
        # Create histogram for min propagation times
        hist_min = alt.Chart(block_stats_pd).mark_bar().encode(
            x=alt.X('min_propagation_ms:Q', 
                   bin=alt.Bin(maxbins=30),
                   title='Min Propagation Time (ms)'),
            y=alt.Y('count()', title='Block Count'),
            tooltip=['count()']
        ).properties(
            title='Distribution of Minimum Block Propagation Times',
            width=600,
            height=300
        )
        
        st.altair_chart(hist_min, use_container_width=True)

    except Exception as e:
        logger.error(f"Error in block distribution analysis: {str(e)}")
        st.error(f"Error creating block distribution charts: {str(e)}") 