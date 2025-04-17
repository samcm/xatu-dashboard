import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
import numpy as np
import logging

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def render_client_analysis_section(data):
    """Render client performance analysis"""
    # Unpack data
    raw_df = data["raw"]
    
    st.header("Client Analysis")
    
    try:
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
        client_chart = alt.Chart(client_counts_pd).mark_bar().encode(
            x=alt.X('count:Q', title='Count'),
            y=alt.Y('meta_consensus_implementation:N', title='Client Implementation', sort='-x'),
            tooltip=['meta_consensus_implementation', 'count']
        ).properties(
            title='Observations by Client Implementation',
            height=min(300, len(client_counts_pd) * 25)
        )
        
        st.altair_chart(client_chart, use_container_width=True)
        
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
            perf_chart = alt.Chart(client_perf_pd).mark_bar().encode(
                x=alt.X('meta_consensus_implementation:N', title='Client Implementation'),
                y=alt.Y('median_ms:Q', title='Median Propagation Time (ms)'),
                color=alt.Color('slow_percentage:Q', 
                               scale=alt.Scale(scheme='redblue', domain=[0, 25]), 
                               title='% Slow Propagations'),
                tooltip=['meta_consensus_implementation', 'median_ms', 'mean_ms', 
                         'p95_ms', 'sample_count', 'slow_percentage']
            ).properties(
                title='Client Propagation Performance',
                width=600,
                height=400
            )
            
            st.altair_chart(perf_chart, use_container_width=True)
        
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
                cdf_chart = alt.Chart(cdf_df).mark_line().encode(
                    x=alt.X('propagation_ms:Q', title='Propagation Time (ms)'),
                    y=alt.Y('percentile:Q', title='Cumulative Probability', scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color('client:N', title='Client Implementation'),
                    tooltip=['client', 'propagation_ms', 'percentile']
                ).properties(
                    title='CDF of Block Arrival Times by Client Implementation',
                    width=600,
                    height=400
                )
                
                # Add vertical lines at key percentiles
                # Create a filtered dataset for the 50th percentile
                p50_data = cdf_df[cdf_df['percentile'].between(0.499, 0.501)]
                p90_data = cdf_df[cdf_df['percentile'].between(0.899, 0.901)]
                
                # Add vertical lines at key percentiles using the filtered data
                p50_rule = alt.Chart(p50_data).mark_rule(color='gray', strokeDash=[5, 5]).encode(
                    x='propagation_ms:Q'
                )
                
                p90_rule = alt.Chart(p90_data).mark_rule(color='gray', strokeDash=[5, 5]).encode(
                    x='propagation_ms:Q'
                )
                
                # Combine charts
                final_chart = cdf_chart + p50_rule + p90_rule
                
                st.altair_chart(final_chart, use_container_width=True)

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
                cdf_chart = alt.Chart(cdf_df).mark_line().encode(
                    x=alt.X('propagation_ms:Q', title='Propagation Time (ms)'),
                    y=alt.Y('percentile:Q', title='Cumulative Probability', scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color('country:N', title='Country'),
                    tooltip=['country', 'propagation_ms', 'percentile']
                ).properties(
                    title='CDF of Block Arrival Times by Country',
                    width=600,
                    height=400
                )
                
                # Add vertical lines at key percentiles
                # Create a filtered dataset for the 50th percentile
                p50_data = cdf_df[cdf_df['percentile'].between(0.499, 0.501)]
                p90_data = cdf_df[cdf_df['percentile'].between(0.899, 0.901)]
                
                # Add vertical lines at key percentiles using the filtered data
                p50_rule = alt.Chart(p50_data).mark_rule(color='gray', strokeDash=[5, 5]).encode(
                    x='propagation_ms:Q'
                )
                
                p90_rule = alt.Chart(p90_data).mark_rule(color='gray', strokeDash=[5, 5]).encode(
                    x='propagation_ms:Q'
                )
                
                # Combine charts
                final_chart = cdf_chart + p50_rule + p90_rule
                
                st.altair_chart(final_chart, use_container_width=True)

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