import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
import logging

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_summary_section(data):
    """Render summary statistics for block arrival times"""
    # Unpack data
    block_stats = data["block_stats"]
    raw_df = data["raw"]
    
    st.header("Summary")
    
    # Key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    # Block count
    with col1:
        unique_blocks = block_stats.shape[0]
        st.metric("Unique Blocks", unique_blocks)
    
    # Observations
    with col2:
        total_observations = raw_df.shape[0]
        st.metric("Total Events", total_observations)
    
    # Avg observations per block
    with col3:
        avg_observations = round(total_observations / unique_blocks, 2)
        st.metric("Avg Events per Block", avg_observations)
    
    # Median propagation time
    with col4:
        median_time = block_stats.select(pl.col("median_propagation_ms").median())[0, 0]
        st.metric("Median Propagation Time", f"{round(median_time, 2)}ms")
    
    # Percentiles table
    st.subheader("Block Propagation Percentiles")
    
    percentiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    percentile_names = ["p10", "p25", "p50", "p75", "p90", "p95", "p99"]
    
    # Calculate percentiles across all blocks (using min propagation time per block)
    percentile_values = [block_stats.select(pl.col("min_propagation_ms").quantile(p))[0, 0] for p in percentiles]
    
    # Create percentile dataframe
    percentile_df = pd.DataFrame({
        "Percentile": [f"{int(p*100)}%" for p in percentiles],
        "Value (ms)": [round(v, 2) for v in percentile_values]
    })
    
    # Display percentiles
    st.dataframe(percentile_df)
    
    # Plot the percentile distribution
    try:
        chart_data = pd.DataFrame({
            "Percentile": percentile_names,
            "Milliseconds": percentile_values
        })
        
        chart = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Percentile:N', title='Percentile', sort=None),
            y=alt.Y('Milliseconds:Q', title='Propagation Time (ms)'),
            tooltip=['Percentile', 'Milliseconds']
        ).properties(
            title='Block Propagation Time Percentiles',
            width=600,
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        logger.error(f"Error creating percentile chart: {str(e)}") 