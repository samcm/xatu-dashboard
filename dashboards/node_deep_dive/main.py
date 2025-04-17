import streamlit as st
import polars as pl
from datetime import datetime, timedelta
import logging
import urllib.parse
from utils import load_xatu_data, load_xatu_data_range
from config import SUPPORTED_NETWORKS, TIME_WINDOWS, DEFAULT_REFRESH_TIME
from chart_utils import create_themed_histogram, create_themed_line

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xatu-dashboard")

# Dashboard-specific settings
REFRESH_TIME = DEFAULT_REFRESH_TIME
DASHBOARD_TITLE = "Xatu Node Deep Dive"

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

def load_node_data(network, node_id, time_window, force_refresh=False):
    """Load data for a specific node"""
    # Calculate date range based on the selected time window
    days = TIME_WINDOWS[time_window]
    
    # Always use historical data (end date is yesterday)
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days)
    
    logger.info(f"Analyzing data for node {node_id} from {start_date} to {end_date}")
    
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
            return None, (start_date, end_date)
        
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
        
        # Extract usernames and node IDs for reference
        df = df.with_columns(
            pl.col("meta_client_name").map_elements(extract_username, return_dtype=pl.Utf8).alias("username"),
            pl.col("meta_client_name").map_elements(extract_node_id, return_dtype=pl.Utf8).alias("node_id")
        )
        
        # Filter data for the specific node using only node_id
        node_df = df.filter(pl.col("node_id") == node_id)
        
        if node_df.height == 0:
            logger.warning(f"No data found for node {node_id}")
            return None, (start_date, end_date)
            
        return node_df, (start_date, end_date)
    
    except Exception as e:
        logger.error(f"Error loading node data: {str(e)}")
        st.error(f"Error loading node data: {str(e)}")
        return None, (start_date, end_date)

def render_node_overview(node_df, node_id):
    """Render an overview of node data"""
    if node_df is None or node_df.height == 0:
        st.warning(f"No data available for node {node_id}")
        return
    
    st.subheader("Node Overview")
    
    with st.container():
        # Basic stats
        col1, col2, col3 = st.columns(3)
        
        # Get node location
        location = "Location Redacted"
        if "meta_client_geo_city" in node_df.columns and "meta_client_geo_country_code" in node_df.columns:
            city = node_df.select("meta_client_geo_city").head(1).item()
            country = node_df.select("meta_client_geo_country_code").head(1).item()
            
            if city and city != "REDACTED" and country:
                location = f"{city}, {country}"
        
        # Client implementation
        implementation = "Unknown"
        if "meta_consensus_implementation" in node_df.columns:
            impl = node_df.select("meta_consensus_implementation").head(1).item()
            if impl:
                implementation = impl
        
        # Client version
        version = "Unknown"
        if "meta_consensus_version" in node_df.columns:
            ver = node_df.select("meta_consensus_version").head(1).item()
            if ver:
                version = ver
        
        # Node name/ID from meta_client_name
        node_name = "Unknown"
        if "node_id" in node_df.columns:
            nid = node_df.select("node_id").head(1).item()
            if nid:
                node_name = nid
        
        # Username
        username = "Unknown"
        if "username" in node_df.columns:
            uname = node_df.select("username").head(1).item()
            if uname:
                username = uname
        
        # Display stats
        with col1:
            st.metric("Client", implementation)
        
        with col2:
            st.metric("Version", version)
        
        with col3:
            st.metric("Total Events", node_df.height)
        
        # Extra info
        st.markdown(f"**Node Name:** {node_name}")
        st.markdown(f"**Location:** {location}")
        st.markdown(f"**User:** {username}")
        
        # Network provider
        if "meta_client_geo_autonomous_system_organization" in node_df.columns:
            asn = node_df.select("meta_client_geo_autonomous_system_organization").head(1).item()
            if asn and asn != "REDACTED":
                st.markdown(f"**Network Provider:** {asn}")
            else:
                st.markdown("**Network Provider:** ASN Redacted")

def render_performance_metrics(node_df):
    """Render performance metrics for the node"""
    if node_df is None or node_df.height == 0:
        return
    
    st.subheader("Performance Metrics")
    
    with st.container():
        # For block events, analyze propagation times
        if "propagation_slot_start_diff" in node_df.columns:
            # Calculate stats for propagation time
            prop_metrics = node_df.select([
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
                    prop_df = node_df.filter(pl.col("propagation_slot_start_diff") < 5000) \
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
                    percentiles = node_df.select([
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

def render_timeline(node_df):
    """Render a timeline of node activity"""
    if node_df is None or node_df.height == 0:
        return
    
    st.subheader("Activity Timeline")
    
    with st.container():
        try:
            # Group data by date to see activity over time
            if "meta_received_at" in node_df.columns:
                # Make sure we safely convert the timestamp column
                try:
                    date_df = node_df.with_columns(
                        pl.col("meta_received_at").cast(pl.Datetime).dt.date().alias("date")
                    ).group_by("date").agg(
                        pl.count().alias("events")
                    ).sort("date")
                except Exception as e:
                    logger.error(f"Error converting timestamps: {str(e)}")
                    # Fallback for string timestamps
                    try:
                        date_df = node_df.with_columns(
                            pl.col("meta_received_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").dt.date().alias("date")
                        ).group_by("date").agg(
                            pl.count().alias("events")
                        ).sort("date")
                    except Exception as inner_e:
                        logger.error(f"Failed to parse timestamps: {str(inner_e)}")
                        st.error("Unable to process timestamp data for timeline")
                        return
                
                # Check if plotly is available
                try:
                    import plotly.express as px
                    has_plotly = True
                except ImportError:
                    has_plotly = False
                    st.warning("Plotly is not installed for timeline visualization")
                
                if has_plotly and date_df.height > 0:
                    # Convert to pandas for plotting
                    date_pd = date_df.to_pandas()
                    
                    # Create line chart
                    fig = create_themed_line(
                        date_pd,
                        x="date",
                        y="events",
                        title="Node Activity Over Time",
                        xaxis_title="Date",
                        yaxis_title="Number of Events"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    # Fallback to table view
                    st.write("Daily event counts:")
                    st.dataframe(date_df.to_pandas())
            else:
                st.info("No timestamp data available for timeline")
        
        except Exception as e:
            logger.error(f"Error creating timeline: {str(e)}")
            st.error(f"Error creating timeline: {str(e)}")

def render(force_refresh=False):
    """Render the node deep dive dashboard"""
    st.title(DASHBOARD_TITLE)
    
    # Get global settings from session state
    network = st.session_state.network
    time_window = st.session_state.time_window
    force_refresh = force_refresh or st.session_state.force_refresh
    
    # Check if node_id is in the URL
    query_params = st.query_params
    node_id = query_params.get("node_id", None)
    username = query_params.get("username", None)
    
    # Show info about the date range we're analyzing
    days = TIME_WINDOWS[time_window]
    end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=days)
    
    st.info(f"📅 Analyzing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} for {network}")
    
    # Check if node_id is provided
    if not node_id:
        st.error("No node ID provided. Please access this dashboard from the User Deep Dive dashboard.")
        
        # Direct entry form as fallback
        with st.form("manual_entry"):
            st.write("Or enter a node ID manually:")
            manual_node_id = st.text_input("Node ID")
            submit = st.form_submit_button("View Node")
            
            if submit and manual_node_id:
                # Update URL params
                st.query_params["node_id"] = manual_node_id
                st.query_params["network"] = network
                st.query_params["time_window"] = time_window
                st.query_params["dashboard"] = "node-deep-dive"
                st.rerun()
        
        return
    
    # Display node ID and username info
    st.markdown(f"**Node ID:** {node_id}")
    if username:
        st.markdown(f"**User:** {username}")
        
        # Add a link back to user deep dive
        user_url = f"/?dashboard=xatu-user-deep-dive&network={network}&time_window={time_window}&username={username}"
        st.markdown(f"[Back to User Deep Dive for {username}]({user_url})")
    
    # Load data for this node
    with st.spinner(f"Loading data for node {node_id}..."):
        node_df, date_range = load_node_data(network, node_id, time_window, force_refresh)
    
    if node_df is None or node_df.height == 0:
        st.warning(f"No data available for node {node_id} in the selected time period")
        return
    
    # Show node data
    try:
        # Overview
        render_node_overview(node_df, node_id)
        
        st.divider()
        
        # Timeline
        render_timeline(node_df)
        
        st.divider()
        
        # Performance metrics
        render_performance_metrics(node_df)
        
    except Exception as e:
        st.error(f"Error rendering node data: {str(e)}")
        logger.error(f"Error rendering node data: {str(e)}")
    
    # Show date range information
    if date_range:
        start_date, end_date = date_range
        st.markdown(f"*Data from {start_date.isoformat()} to {end_date.isoformat()}. Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

if __name__ == "__main__":
    render() 