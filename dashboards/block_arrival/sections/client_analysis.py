import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
import logging
from chart_utils import create_themed_bar, create_themed_line

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_client_analysis_section(data):
    """Render client performance analysis"""
    # Unpack data
    raw_df = data["raw"]
    
    st.header("Client Analysis")
    
    try:
        with st.container():
            # Client implementation distribution
            st.subheader("Client Implementation Distribution")
            
            # Count by client implementation
            client_counts = raw_df.group_by("meta_consensus_implementation").agg(
                pl.count().alias("count")
            ).sort("count", descending=True)
            
            # Convert to pandas for visualization
            client_counts_pd = client_counts.to_pandas()
            
            # Limit to top 10 for readability if needed
            if client_counts_pd.shape[0] > 10:
                client_counts_pd = client_counts_pd.head(10)
            
            # Create bar chart
            fig = create_themed_bar(
                client_counts_pd,
                x="count",
                y="meta_consensus_implementation",
                title="Observations by Client Implementation",
                xaxis_title="Count",
                yaxis_title="Client Implementation"
            )
            
            # Adjust height based on number of clients
            fig.update_layout(height=min(400, len(client_counts_pd) * 30))
            
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        with st.container():
            # Client performance analysis
            st.subheader("Client Propagation Performance")
            
            # Calculate performance metrics by client implementation
            client_perf = raw_df.group_by("meta_consensus_implementation").agg(
                pl.col("capped_diff_ms").mean().alias("mean_ms"),
                pl.col("capped_diff_ms").median().alias("median_ms"),
                pl.col("capped_diff_ms").quantile(0.95).alias("p95_ms"),
                pl.count().alias("sample_count"),
                pl.col("is_slow_propagation").mean().alias("slow_ratio")
            ).sort("median_ms")
            
            # Add slow ratio as percentage
            client_perf = client_perf.with_columns(
                (pl.col("slow_ratio") * 100).round(1).alias("slow_percentage")
            )
            
            # Display performance table
            st.dataframe(client_perf.to_pandas())
            
            # Filter to clients with sufficient samples for chart
            client_perf_pd = client_perf.filter(
                pl.col("sample_count") > max(10, raw_df.shape[0] * 0.01)
            ).to_pandas()
            
            # Create performance comparison chart
            if client_perf_pd.shape[0] > 0:
                # Create performance comparison chart
                fig = create_themed_bar(
                    client_perf_pd,
                    x="meta_consensus_implementation",
                    y="median_ms",
                    title="Client Propagation Performance",
                    xaxis_title="Client Implementation",
                    yaxis_title="Median Propagation Time (ms)",
                    color="slow_percentage"
                )
                
                # Update color scale to match original
                fig.update_traces(
                    marker=dict(
                        colorscale="RdBu_r",
                        colorbar=dict(title="% Slow Propagations"),
                        cmin=0,
                        cmax=25
                    )
                )
                
                # Add hover data
                fig.update_traces(
                    hovertemplate="<b>%{x}</b><br>" +
                                 "Median: %{y:.2f} ms<br>" +
                                 "Mean: %{customdata[0]:.2f} ms<br>" +
                                 "P95: %{customdata[1]:.2f} ms<br>" +
                                 "Samples: %{customdata[2]}<br>" +
                                 "Slow %%: %{customdata[3]:.1f}%",
                    customdata=client_perf_pd[['mean_ms', 'p95_ms', 'sample_count', 'slow_percentage']].values
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        with st.container():
            # CDF of propagation times by client implementation
            st.subheader("Block Arrival Time CDF by Client Implementation")
            
            # Get top clients by observation count
            top_clients = client_counts.head(5).select("meta_consensus_implementation").to_series().to_list()
            
            # Filter data to top clients
            clients_data = raw_df.filter(pl.col("meta_consensus_implementation").is_in(top_clients))
            
            # Create CDF data for each client
            try:
                cdf_data = []
                
                for client in top_clients:
                    # Filter to just this client
                    client_data = clients_data.filter(pl.col("meta_consensus_implementation") == client)
                    
                    if client_data.shape[0] > 0:
                        # Get propagation times and round to nearest 50ms
                        times = client_data.select(
                            (pl.col("capped_diff_ms").round(0) / 50).round(0) * 50
                        ).to_series().to_list()
                        times.sort()
                        
                        # Create cumulative distribution with binning
                        unique_times = []
                        percentiles = []
                        
                        current_time = None
                        for i, time in enumerate(times):
                            if time != current_time:
                                unique_times.append(time)
                                percentiles.append((i + 1) / len(times))
                                current_time = time
                            elif i == len(times) - 1:
                                # Always include the last percentile
                                percentiles[-1] = (i + 1) / len(times)
                        
                        # Create final CDF data points
                        for i, (time, pct) in enumerate(zip(unique_times, percentiles)):
                            cdf_data.append({
                                "client": client,
                                "propagation_ms": time,
                                "percentile": pct
                            })
                
                # Convert to pandas for visualization
                if cdf_data:
                    cdf_df = pd.DataFrame(cdf_data)
                    
                    # Create the CDF chart
                    fig = create_themed_line(
                        cdf_df,
                        x="propagation_ms",
                        y="percentile",
                        title="CDF of Block Arrival Times by Client Implementation",
                        xaxis_title="Propagation Time (ms)",
                        yaxis_title="Cumulative Probability",
                        color="client"
                    )
                    
                    # Set y-axis range to match original
                    fig.update_yaxes(range=[0, 1])
                    
                    # Add vertical lines at key percentiles
                    p50_data = cdf_df[cdf_df['percentile'].between(0.499, 0.501)]
                    p90_data = cdf_df[cdf_df['percentile'].between(0.899, 0.901)]
                    
                    # Add vertical lines for p50
                    for _, row in p50_data.iterrows():
                        fig.add_vline(
                            x=row['propagation_ms'],
                            line=dict(color='gray', width=1, dash='dash'),
                            annotation_text=f"p50: {row['propagation_ms']:.0f}ms",
                            annotation_position="top right"
                        )
                    
                    # Add vertical lines for p90
                    for _, row in p90_data.iterrows():
                        fig.add_vline(
                            x=row['propagation_ms'],
                            line=dict(color='gray', width=1, dash='dash'),
                            annotation_text=f"p90: {row['propagation_ms']:.0f}ms",
                            annotation_position="top right"
                        )
                    
                    st.plotly_chart(fig, use_container_width=True)

                    # Show key percentiles table
                    st.subheader("Key Block Arrival Percentiles by Client Implementation")
                    
                    # Calculate key percentiles for each client
                    percentiles_by_client = []
                    for client in top_clients:
                        client_data = clients_data.filter(pl.col("meta_consensus_implementation") == client)
                        if client_data.shape[0] > 0:
                            p50 = client_data.select(pl.col("capped_diff_ms").quantile(0.5))[0, 0]
                            p90 = client_data.select(pl.col("capped_diff_ms").quantile(0.9))[0, 0]
                            p99 = client_data.select(pl.col("capped_diff_ms").quantile(0.99))[0, 0]
                            
                            percentiles_by_client.append({
                                "Client": client,
                                "50th Percentile (ms)": round(p50, 2),
                                "90th Percentile (ms)": round(p90, 2),
                                "99th Percentile (ms)": round(p99, 2),
                                "Sample Count": client_data.shape[0]
                            })
                    
                    if percentiles_by_client:
                        st.dataframe(pd.DataFrame(percentiles_by_client))
                else:
                    st.warning("Not enough data to create CDF visualization for clients.")
                    
            except Exception as e:
                logger.error(f"Error creating client CDF visualization: {str(e)}")
                st.error(f"Error creating client CDF visualization: {str(e)}")
        
        st.divider()
        
        with st.container():
            # Geographic analysis
            st.subheader("Geographic Distribution")
            
            # Count by country
            geo_counts = raw_df.group_by("meta_client_geo_country").agg(
                pl.count().alias("count")
            ).sort("count", descending=True)
            
            # Geographic performance
            geo_perf = raw_df.group_by("meta_client_geo_country").agg(
                pl.col("capped_diff_ms").median().alias("median_ms"),
                pl.count().alias("sample_count")
            ).sort("median_ms")
            
            # Filter out countries with too few samples
            geo_perf = geo_perf.filter(pl.col("sample_count") > max(10, raw_df.shape[0] * 0.01))

            # CDF of propagation times by country
            st.subheader("Block Arrival Time CDF by Top 20 Country")
            
            # Get top countries by observation count
            top_countries = geo_counts.head(10).select("meta_client_geo_country").to_series().to_list()
            
            # Filter data to top countries
            countries_data = raw_df.filter(pl.col("meta_client_geo_country").is_in(top_countries))
            
            # Create CDF data for each country
            try:
                cdf_data = []
                
                for country in top_countries:
                    # Filter to just this country
                    country_data = countries_data.filter(pl.col("meta_client_geo_country") == country)
                    
                    if country_data.shape[0] > 0:
                        # Get propagation times and round to nearest 50ms
                        times = country_data.select(
                            (pl.col("capped_diff_ms").round(0) / 50).round(0) * 50
                        ).to_series().to_list()
                        times.sort()
                        
                        # Create cumulative distribution with binning
                        unique_times = []
                        percentiles = []
                        
                        current_time = None
                        for i, time in enumerate(times):
                            if time != current_time:
                                unique_times.append(time)
                                percentiles.append((i + 1) / len(times))
                                current_time = time
                            elif i == len(times) - 1:
                                # Always include the last percentile
                                percentiles[-1] = (i + 1) / len(times)
                        
                        # Create final CDF data points
                        for i, (time, pct) in enumerate(zip(unique_times, percentiles)):
                            cdf_data.append({
                                "country": country,
                                "propagation_ms": time,
                                "percentile": pct
                            })
                
                # Convert to pandas for visualization
                if cdf_data:
                    cdf_df = pd.DataFrame(cdf_data)
                    
                    # Create the CDF chart
                    fig = create_themed_line(
                        cdf_df,
                        x="propagation_ms",
                        y="percentile",
                        title="CDF of Block Arrival Times by Country",
                        xaxis_title="Propagation Time (ms)",
                        yaxis_title="Cumulative Probability",
                        color="country"
                    )
                    
                    # Set y-axis range to match original
                    fig.update_yaxes(range=[0, 1])
                    
                    # Add vertical lines at key percentiles
                    p50_data = cdf_df[cdf_df['percentile'].between(0.499, 0.501)]
                    p90_data = cdf_df[cdf_df['percentile'].between(0.899, 0.901)]
                    
                    # Add vertical lines for p50
                    for _, row in p50_data.iterrows():
                        fig.add_vline(
                            x=row['propagation_ms'],
                            line=dict(color='gray', width=1, dash='dash'),
                            annotation_text=f"p50: {row['propagation_ms']:.0f}ms",
                            annotation_position="top right"
                        )
                    
                    # Add vertical lines for p90
                    for _, row in p90_data.iterrows():
                        fig.add_vline(
                            x=row['propagation_ms'],
                            line=dict(color='gray', width=1, dash='dash'),
                            annotation_text=f"p90: {row['propagation_ms']:.0f}ms",
                            annotation_position="top right"
                        )
                    
                    st.plotly_chart(fig, use_container_width=True)

                    # Show key percentiles table
                    st.subheader("Key Block Arrival Percentiles by Country")
                    
                    # Calculate key percentiles for each country
                    percentiles_by_country = []
                    for country in top_countries:
                        country_data = countries_data.filter(pl.col("meta_client_geo_country") == country)
                        if country_data.shape[0] > 0:
                            p50 = country_data.select(pl.col("capped_diff_ms").quantile(0.5))[0, 0]
                            p90 = country_data.select(pl.col("capped_diff_ms").quantile(0.9))[0, 0]
                            p99 = country_data.select(pl.col("capped_diff_ms").quantile(0.99))[0, 0]
                            
                            percentiles_by_country.append({
                                "Country": country,
                                "50th Percentile (ms)": round(p50, 2),
                                "90th Percentile (ms)": round(p90, 2),
                                "99th Percentile (ms)": round(p99, 2),
                                "Sample Count": country_data.shape[0]
                            })
                    
                    if percentiles_by_country:
                        st.dataframe(pd.DataFrame(percentiles_by_country))
                else:
                    st.warning("Not enough data to create CDF visualization.")
                    
            except Exception as e:
                logger.error(f"Error creating CDF visualization: {str(e)}")
                st.error(f"Error creating CDF visualization: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in client analysis: {str(e)}")
        st.error(f"Error analyzing client performance: {str(e)}")