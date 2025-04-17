import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
import numpy as np
import logging

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_hourly_trends_section(data, network):
    """Render hourly trend analysis"""
    # Unpack data
    block_stats = data["block_stats"]
    
    st.header("Hourly Trends")
    
    try:
        # Aggregate by hour
        hourly_stats = block_stats.group_by("hour").agg(
            pl.col("min_propagation_ms").mean().alias("mean_min_ms"),
            pl.col("min_propagation_ms").median().alias("median_min_ms"),
            pl.col("min_propagation_ms").quantile(0.9).alias("p90_min_ms"),
            pl.count().alias("block_count")
        ).sort("hour")
        
        # Convert to pandas for visualization
        hourly_pd = hourly_stats.to_pandas()
        
        # Create dual-axis visualization
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Propagation Time by Hour")
            
            # Melt for line chart
            hourly_long = pd.melt(
                hourly_pd,
                id_vars=["hour"],
                value_vars=["mean_min_ms", "median_min_ms", "p90_min_ms"],
                var_name="metric",
                value_name="milliseconds"
            )
            
            # Create line chart
            line_chart = alt.Chart(hourly_long).mark_line().encode(
                x=alt.X('hour:O', title='Hour of Day'),
                y=alt.Y('milliseconds:Q', title='Propagation Time (ms)'),
                color=alt.Color('metric:N', 
                               scale=alt.Scale(domain=['median_min_ms', 'mean_min_ms', 'p90_min_ms'],
                                              range=['#1f77b4', '#ff7f0e', '#d62728']),
                               title='Metric'),
                tooltip=['hour', 'metric', 'milliseconds']
            ).properties(
                title=f'Block Propagation by Hour ({network})',
                width=600,
                height=300
            )
            
            st.altair_chart(line_chart, use_container_width=True)
        
        with col2:
            st.subheader("Block Count by Hour")
            
            # Create bar chart for block counts
            bar_chart = alt.Chart(hourly_pd).mark_bar().encode(
                x=alt.X('hour:O', title='Hour of Day'),
                y=alt.Y('block_count:Q', title='Block Count'),
                tooltip=['hour', 'block_count']
            ).properties(
                title=f'Blocks by Hour ({network})',
                width=600,
                height=300
            )
            
            st.altair_chart(bar_chart, use_container_width=True)
        
        # Correlation analysis
        st.subheader("Propagation vs Block Count Correlation")
        
        # Calculate correlation coefficient
        correlation = np.corrcoef(hourly_pd['block_count'], hourly_pd['median_min_ms'])[0, 1]
        st.metric("Correlation Coefficient", round(correlation, 4))
        
        # Explanation based on correlation strength
        if abs(correlation) > 0.5:
            if correlation > 0:
                st.info("There is a moderate to strong positive correlation between block count and propagation time, suggesting higher network load may increase block propagation times.")
            else:
                st.info("There is a moderate to strong negative correlation between block count and propagation time, suggesting the network may be more efficient during periods of higher activity.")
        else:
            st.info("There is minimal correlation between block count and propagation time, suggesting block propagation is relatively stable regardless of network load.")
        
        # Scatter plot visualization
        scatter_plot = alt.Chart(hourly_pd).mark_circle(size=60).encode(
            x=alt.X('block_count:Q', title='Block Count'),
            y=alt.Y('median_min_ms:Q', title='Median Min Propagation Time (ms)'),
            color=alt.Color('hour:O', title='Hour',
                           scale=alt.Scale(scheme='viridis')),
            tooltip=['hour', 'block_count', 'median_min_ms']
        ).properties(
            title='Block Count vs Propagation Time Correlation',
            width=600,
            height=300
        )
        
        st.altair_chart(scatter_plot, use_container_width=True)
        
    except Exception as e:
        logger.error(f"Error in hourly trends analysis: {str(e)}")
        st.error(f"Error creating hourly analysis: {str(e)}")
        st.exception(e) 