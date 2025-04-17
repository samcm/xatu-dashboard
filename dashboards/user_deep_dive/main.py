import streamlit as st
import polars as pl
from datetime import datetime, timedelta
import logging
import urllib.parse
from utils import load_xatu_data, load_xatu_data_range
from config import SUPPORTED_NETWORKS, TIME_WINDOWS, DEFAULT_REFRESH_TIME
from chart_utils import create_themed_histogram

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xatu-dashboard")

# Dashboard-specific settings
REFRESH_TIME = DEFAULT_REFRESH_TIME
DASHBOARD_TITLE = "Xatu User Deep Dive"

def extract_username(meta_client_name):
    """Extract username from meta_client_name field
    
    Format is typically: 'ethpandaops/mainnet/sigma-mainnet-prysm-reth-001'
                      or 'pub-asn-city/robustdigress65/hashed-446c3e10'
    We want to extract the middle part (the username).
    """
    try:
        # Handle bytes type (convert to string)
        if isinstance(meta_client_name, bytes):
            meta_client_name = meta_client_name.decode('utf-8')
        
        if meta_client_name is None or not isinstance(meta_client_name, str):
            return None
        
        # Trim any whitespace
        meta_client_name = meta_client_name.strip()
        
        # Skip empty strings
        if not meta_client_name:
            return None
        
        # Handle special cases
        if meta_client_name.lower() in ['unknown', 'redacted', 'none', 'null']:
            return None
        
        # Split by / and extract the appropriate part based on format
        parts = meta_client_name.split('/')
        
        # Need at least 2 parts
        if len(parts) < 2:
            return None
            
        # Check common prefixes to determine format
        if parts[0].lower() in ['ethpandaops', 'pub-asn-city', 'pub-noasn-city']:
            # Format is prefix/username/... - extract the second part
            if len(parts) >= 2 and len(parts[1]) >= 2:
                username = parts[1].strip()
                
                # Skip if it's just "mainnet"
                if username.lower() == "mainnet" and len(parts) >= 3:
                    # If the second part is "mainnet", try the third part
                    return parts[2].strip()
                
                return username
        
        # If no standard format matched, just return the first non-empty part that isn't a common prefix
        for part in parts:
            part = part.strip()
            if part and len(part) >= 2 and part.lower() not in ['ethpandaops', 'mainnet', 'pub-asn-city', 'pub-noasn-city']:
                return part
        
        # No valid username found
        return None
    
    except Exception as e:
        logger.error(f"Error extracting username from {meta_client_name}: {str(e)}")
        return None

def extract_node_id(meta_client_name):
    """Extract node ID from meta_client_name field
    
    Format is typically: 'ethpandaops/mainnet/sigma-mainnet-prysm-reth-001'
                      or 'pub-asn-city/robustdigress65/hashed-446c3e10'
    We want to extract the third part (the node ID).
    """
    try:
        # Handle bytes type (convert to string)
        if isinstance(meta_client_name, bytes):
            meta_client_name = meta_client_name.decode('utf-8')
        
        if meta_client_name is None or not isinstance(meta_client_name, str):
            return None
        
        # Trim any whitespace
        meta_client_name = meta_client_name.strip()
        
        # Skip empty strings
        if not meta_client_name:
            return None
        
        # Split by / and extract the appropriate part based on format
        parts = meta_client_name.split('/')
        
        # Need at least 3 parts for a node ID
        if len(parts) >= 3:
            # Format is 'prefix/username/node_id' - extract the third part
            node_id = parts[2].strip()
            if node_id:
                return node_id
        
        # Fallback: if we have exactly 2 parts, return the second as the node ID
        if len(parts) == 2 and parts[1].strip():
            return parts[1].strip()
            
        # No valid node ID found
        return None
    
    except Exception as e:
        logger.error(f"Error extracting node ID from {meta_client_name}: {str(e)}")
        return None

def load_usernames(network, time_window, force_refresh=False):
    """Load all usernames from beacon API data"""
    # Calculate date range based on the selected time window
    days = TIME_WINDOWS[time_window]
    
    # Always use historical data (end date is yesterday)
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days)
    
    logger.info(f"Analyzing data from {start_date} to {end_date}")
    
    # Load data
    try:
        df = load_xatu_data_range(
            network,
            "beacon_api_eth_v1_events_block",
            start_date,
            end_date,
            use_cache=not force_refresh
        )
        
        if df is None or len(df) == 0:
            logger.warning(f"No data available for {network} in the selected time period")
            return None, None, (start_date, end_date)
        
        # Convert all string/byte columns to proper strings
        string_cols = [
            "meta_client_name", 
            "meta_client_geo_city", 
            "meta_client_geo_country", 
            "meta_client_geo_country_code", 
            "meta_client_geo_autonomous_system_organization",
            "meta_consensus_implementation",
            "meta_consensus_version"
        ]
        
        # Only convert columns that exist in the dataframe
        for col in string_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col).is_null())
                        .then(None)
                        .otherwise(
                            pl.when(pl.col(col).map_elements(lambda x: isinstance(x, bytes), return_dtype=pl.Boolean))
                                .then(pl.col(col).map_elements(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x, return_dtype=pl.Utf8))
                                .otherwise(pl.col(col).cast(pl.Utf8))
                        )
                        .alias(col)
                )
        
        # Extract usernames with proper return dtype
        df = df.with_columns(
            pl.col("meta_client_name").map_elements(extract_username, return_dtype=pl.Utf8).alias("username")
        )
        
        # Also extract node IDs
        df = df.with_columns(
            pl.col("meta_client_name").map_elements(extract_node_id, return_dtype=pl.Utf8).alias("node_id")
        )
        
        # Get unique usernames directly with Python - avoid any polars groupby operations
        # First, get all non-null username values as a plain Python list
        username_series = df.select(pl.col("username")).filter(
            pl.col("username").is_not_null()
        ).to_series()
        
        # Convert to a Python list
        username_list = username_series.to_list()
        
        if not username_list:
            logger.warning(f"No valid usernames found in the data for {network}")
            return None, df, (start_date, end_date)
        
        # Use Python's set to get unique values and avoid Polars operations
        unique_usernames = list(set(username_list))
        
        # Sort the usernames
        unique_usernames.sort()
        
        logger.info(f"Found {len(unique_usernames)} unique usernames")
        sample = unique_usernames[:5] if len(unique_usernames) >= 5 else unique_usernames
        logger.info(f"Sample usernames: {sample}")
        
        return unique_usernames, df, (start_date, end_date)
    
    except Exception as e:
        logger.error(f"Error loading usernames: {str(e)}")
        st.error(f"Error loading usernames: {str(e)}")
        return None, None, (start_date, end_date)

def load_user_data(df, username):
    """Filter data for a specific user"""
    if df is None:
        return None
    
    user_df = df.filter(pl.col("username") == username)
    return user_df

def render_user_selector(usernames, selected_username=None):
    """Render a selector for usernames"""
    if usernames is None or len(usernames) == 0:
        st.warning("No users found in the selected time period")
        return None
    
    # Use selectbox for username selection
    selected = st.selectbox(
        "Select Xatu User", 
        options=usernames,
        index=0 if selected_username is None else usernames.index(selected_username) if selected_username in usernames else 0,
        format_func=lambda x: f"{x} ({usernames.index(x) + 1}/{len(usernames)})",
    )
    
    return selected

def render_user_overview(user_df, username):
    """Render an overview of user data"""
    if user_df is None or user_df.height == 0:
        st.warning(f"No data available for user {username}")
        return
    
    st.subheader(f"Overview for {username}")
    
    with st.container():
        # Basic stats
        col1, col2, col3 = st.columns(3)
        
        # Get unique node count based on node_id
        node_count = user_df.select(pl.col("node_id").n_unique()).item()
        
        # Get node locations
        locations = user_df.filter(
            ~pl.col("meta_client_geo_city").is_null() &
            (pl.col("meta_client_geo_city") != "REDACTED")
        ).select(
            pl.concat_str(
                pl.col("meta_client_geo_city"),
                pl.lit(", "),
                pl.col("meta_client_geo_country_code")
            ).alias("location")
        ).unique().select("location").to_series().to_list()
        
        # Client implementations
        implementations = user_df.select(pl.col("meta_consensus_implementation")).unique().to_series().to_list()
        
        # Display stats
        with col1:
            st.metric("Node Count", node_count)
        
        with col2:
            st.metric("Total Events", user_df.height)
        
        with col3:
            st.metric("Client Implementations", ", ".join(implementations))
        
        # Show locations if available
        if locations:
            st.write("Node Locations:")
            st.write(", ".join(locations))
        else:
            st.write("Node Locations: Not available (redacted)")
        
        # Show client versions
        versions = user_df.select(pl.col("meta_consensus_version")).unique().to_series().to_list()
        st.write("Client Versions:")
        st.write(", ".join(versions))

def render_node_details(user_df):
    """Render details for individual nodes of the user"""
    if user_df is None or user_df.height == 0:
        return
    
    st.subheader("Node Details")
    
    with st.container():
        # Add info about node deep dive links
        network = st.session_state.network
        username = user_df.select("username").head(1).item() if user_df.height > 0 else None
        
        if username:
            st.info("Click on a Node ID in the table below to view detailed metrics for that specific node.")
        
        # Handle missing node_id column
        if "node_id" not in user_df.columns:
            user_df = user_df.with_columns(
                pl.col("meta_client_name").map_elements(extract_node_id, return_dtype=pl.Utf8).alias("node_id")
            )
        
        try:
            # Filter out null node_ids to avoid group_by issues
            filtered_df = user_df.filter(pl.col("node_id").is_not_null())
            
            if filtered_df.height == 0:
                st.warning("No valid node data available")
                return
            
            # Get unique node IDs
            unique_ids = list(set(filtered_df.select("node_id").to_series().to_list()))
            logger.info(f"Found {len(unique_ids)} unique node IDs")
            
            # Create a list to hold node data
            rows = []
            
            # Process each node ID
            for node_id in unique_ids:
                node_rows = filtered_df.filter(pl.col("node_id") == node_id)
                
                # Get the first row for this node
                first_row = node_rows.head(1)
                
                # Add basic data
                row_data = {
                    "Node ID": node_id,
                    "Events": node_rows.height
                }
                
                # Add client implementation if available
                if "meta_consensus_implementation" in first_row.columns:
                    row_data["Implementation"] = first_row.select("meta_consensus_implementation").item()
                
                # Add client version if available
                if "meta_consensus_version" in first_row.columns:
                    row_data["Version"] = first_row.select("meta_consensus_version").item()
                
                # Add location if available
                if "meta_client_geo_city" in first_row.columns and "meta_client_geo_country_code" in first_row.columns:
                    city = first_row.select("meta_client_geo_city").item()
                    country = first_row.select("meta_client_geo_country_code").item()
                    
                    if city and city != "REDACTED" and country:
                        row_data["Location"] = f"{city}, {country}"
                    else:
                        row_data["Location"] = "Location Redacted"
                
                # Add network provider if available
                if "meta_client_geo_autonomous_system_organization" in first_row.columns:
                    asn = first_row.select("meta_client_geo_autonomous_system_organization").item()
                    
                    if asn and asn != "REDACTED":
                        row_data["Network Provider"] = asn
                    else:
                        row_data["Network Provider"] = "ASN Redacted"
                
                rows.append(row_data)
            
            # Create DataFrame from the rows and sort by event count
            if rows:
                # Convert to DataFrame
                nodes_df = pl.from_dicts(rows)
                nodes_df = nodes_df.sort("Events", descending=True)
                
                # Create node deep dive links
                # Convert to pandas for easier manipulation and display
                nodes_pd = nodes_df.to_pandas()
                
                # Create clickable links for Node IDs
                nodes_pd["Node ID"] = nodes_pd.apply(
                    lambda row: f"[{row['Node ID']}](/?dashboard=node-deep-dive&network={network}&node_id={row['Node ID']}&username={username})",
                    axis=1
                )
                
                # Display with markdown for clickable links
                st.markdown("### Node List")
                st.markdown("Click on a Node ID to view detailed metrics:")
                st.markdown(nodes_pd.to_markdown(index=False), unsafe_allow_html=True)
                
                # As a fallback, also show as a regular table
                st.dataframe(nodes_df.to_pandas(), use_container_width=True)
            else:
                st.warning("No node data to display")
        
        except Exception as e:
            st.error(f"Error displaying node details: {str(e)}")
            logger.error(f"Error in render_node_details: {str(e)}")

def render_performance_metrics(user_df):
    """Render performance metrics for the user"""
    if user_df is None or user_df.height == 0:
        return
    
    st.subheader("Performance Metrics")
    
    with st.container():
        # For block events, analyze propagation times
        if "propagation_slot_start_diff" in user_df.columns:
            # Calculate stats for propagation time
            prop_metrics = user_df.select([
                pl.min("propagation_slot_start_diff").alias("min"),
                pl.mean("propagation_slot_start_diff").alias("mean"),
                pl.median("propagation_slot_start_diff").alias("median"),
                pl.quantile("propagation_slot_start_diff", 0.9).alias("p90"),
            ])
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Min Propagation", f"{prop_metrics.item(0, 'min'):.2f}ms")
            
            with col2:
                st.metric("Mean Propagation", f"{prop_metrics.item(0, 'mean'):.2f}ms")
            
            with col3:
                st.metric("Median Propagation", f"{prop_metrics.item(0, 'median'):.2f}ms")
            
            with col4:
                st.metric("P90 Propagation", f"{prop_metrics.item(0, 'p90'):.2f}ms")
            
            # Create a histogram of propagation times
            try:
                # Try to import required libraries
                import pandas as pd
                
                # Check if plotly is available
                try:
                    import plotly.express as px
                    has_plotly = True
                except ImportError:
                    has_plotly = False
                    st.warning("Plotly is not installed. Install with: `pip install plotly`")
                
                if has_plotly:
                    # Prepare the data - limit to reasonable values for better visualization
                    prop_df = user_df.filter(pl.col("propagation_slot_start_diff") < 5000) \
                                .select("propagation_slot_start_diff") \
                                .to_pandas()
                    
                    # Create histogram
                    try:
                        fig = create_themed_histogram(
                            prop_df,
                            x="propagation_slot_start_diff",
                            nbins=50,
                            title="Distribution of Block Propagation Times",
                            xaxis_title="Propagation Time (ms)",
                            yaxis_title="Count"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as plot_error:
                        st.error(f"Error creating plot: {str(plot_error)}")
                        logger.error(f"Plot error: {str(plot_error)}")
                else:
                    # Fallback to showing summary stats if no plotly
                    st.write("Propagation time distribution summary:")
                    percentiles = user_df.select([
                        pl.quantile("propagation_slot_start_diff", 0.1).alias("p10"),
                        pl.quantile("propagation_slot_start_diff", 0.25).alias("p25"),
                        pl.quantile("propagation_slot_start_diff", 0.5).alias("p50"),
                        pl.quantile("propagation_slot_start_diff", 0.75).alias("p75"),
                        pl.quantile("propagation_slot_start_diff", 0.9).alias("p90"),
                        pl.quantile("propagation_slot_start_diff", 0.95).alias("p95"),
                        pl.quantile("propagation_slot_start_diff", 0.99).alias("p99"),
                    ])
                    
                    st.dataframe(percentiles.to_pandas())
                    
            except Exception as e:
                logger.error(f"Error creating propagation histogram: {str(e)}")
                st.error(f"Error creating propagation histogram: {str(e)}")
        else:
            st.info("No propagation metrics available in the data")

def render(force_refresh=False):
    """Render the user deep dive dashboard"""
    st.title(DASHBOARD_TITLE)
    
    # Get global settings from session state
    network = st.session_state.network
    time_window = st.session_state.time_window
    force_refresh = force_refresh or st.session_state.force_refresh
    
    # Check if a username is in the URL
    query_params = st.query_params
    selected_username = query_params.get("username", None)
    
    # Show info about the date range we're analyzing
    days = TIME_WINDOWS[time_window]
    
    # Make sure our date range makes sense (always looking at historical data)
    end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=days)
    
    st.info(f"📅 Analyzing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} for {network}")
    
    # Load usernames
    with st.spinner("Loading Xatu users..."):
        usernames, full_df, date_range = load_usernames(network, time_window, force_refresh)
    
    if usernames is None or len(usernames) == 0:
        # More detailed error message
        st.error(f"""No valid usernames found for {network} in the selected time period. 
        
This is likely due to one of these reasons:
1. No data is available for this time period
2. The data exists but does not contain valid `meta_client_name` values
3. All `meta_client_name` values have invalid format

Try:
- A different network (current: {network})
- A different time window (current: {time_window})
- Checking logs for more details
        """)
        
        # If we have full_df but no usernames, show some sample data for debugging
        if full_df is not None and full_df.height > 0:
            with st.expander("Debug: Sample Data"):
                st.write("Sample raw data (first 5 rows):")
                
                # Create a simpler dataframe for display
                debug_cols = ["meta_client_name", "username"] 
                debug_cols = [col for col in debug_cols if col in full_df.columns]
                
                if debug_cols:
                    sample_df = full_df.select(debug_cols).head(5)
                    st.dataframe(sample_df.to_pandas())
                else:
                    st.write("No relevant columns found in the data")
        
        return
    
    # Create a 3-column layout for filter controls
    filter_col1, filter_col2, spacer = st.columns([2, 2, 3])
    
    with filter_col1:
        # Show username selector
        new_username = render_user_selector(usernames, selected_username)
        
        # Update URL if username changed
        if new_username != selected_username:
            # Update URL params
            params = dict(query_params)
            params["username"] = new_username
            params["network"] = network
            params["time_window"] = time_window
            params["dashboard"] = "xatu-user-deep-dive"
            
            # Apply new params
            for key, value in params.items():
                st.query_params[key] = value
            
            # Set the selected username
            selected_username = new_username
    
    # If no username is selected, show instructions
    if not selected_username:
        st.info("Please select a user to view their data")
        return
    
    # Load data for selected user
    with st.spinner(f"Loading data for {selected_username}..."):
        user_df = load_user_data(full_df, selected_username)
    
    if user_df is None or user_df.height == 0:
        st.warning(f"No data available for user {selected_username} in the selected time period")
        return
    
    # Show user data
    try:
        # Overview
        render_user_overview(user_df, selected_username)
        
        st.divider()
        
        # Node details
        render_node_details(user_df)
        
        st.divider()
        
        # Performance metrics
        render_performance_metrics(user_df)
        
    except Exception as e:
        st.error(f"Error rendering user data: {str(e)}")
        logger.error(f"Error rendering user data: {str(e)}")
    
    # Show date range information
    if date_range:
        start_date, end_date = date_range
        st.markdown(f"*Data from {start_date.isoformat()} to {end_date.isoformat()}. Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

if __name__ == "__main__":
    render() 