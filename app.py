import streamlit as st
import polars as pl
import importlib
from pathlib import Path
from datetime import datetime, timedelta
import urllib.parse
from config import (
    SUPPORTED_NETWORKS, TIME_WINDOWS, DEFAULT_REFRESH_TIME,
    DASHBOARDS, DEFAULT_TIME_WINDOW_INDEX
)

# Set page config
st.set_page_config(page_title="Xatu Dashboard", layout="wide")

# Get URL parameters
query_params = st.query_params

# Initialize session state for global settings
if "force_refresh" not in st.session_state:
    st.session_state.force_refresh = False
if "network" not in st.session_state:
    # Check if network is in URL params
    if "network" in query_params and query_params["network"] in SUPPORTED_NETWORKS:
        st.session_state.network = query_params["network"]
    else:
        st.session_state.network = SUPPORTED_NETWORKS[0]
if "time_window" not in st.session_state:
    # Check if time_window is in URL params
    if "time_window" in query_params and query_params["time_window"] in TIME_WINDOWS:
        st.session_state.time_window = query_params["time_window"]
    else:
        st.session_state.time_window = list(TIME_WINDOWS.keys())[DEFAULT_TIME_WINDOW_INDEX]
if "current_dashboard" not in st.session_state:
    # Check if dashboard is in URL params
    if "dashboard" in query_params:
        dashboard_name = query_params["dashboard"]
        if dashboard_name == "home":
            st.session_state.current_dashboard = "Home"
        elif dashboard_name in DASHBOARDS:
            st.session_state.current_dashboard = dashboard_name
        else:
            st.session_state.current_dashboard = "Home"
    else:
        st.session_state.current_dashboard = "Home"

# Function to create dashboard URL
def get_dashboard_url(dashboard_name):
    dashboard_slug = dashboard_name.lower().replace(" ", "-") if dashboard_name != "Home" else "home"
    params = {
        "dashboard": dashboard_slug,
        "network": st.session_state.network,
        "time_window": st.session_state.time_window
    }
    return f"?{urllib.parse.urlencode(params)}"

# Custom CSS for logo outline
st.markdown("""
<style>
.logo-container {
    padding: 10px;
    border-radius: 10px;
    border: 2px solid white;
    display: inline-block;
    background: transparent;
    margin-bottom: 15px;
}
</style>
""", unsafe_allow_html=True)

# Sidebar with dashboard selection
# Add Xatu logo, centered
st.sidebar.image("assets/ethpandaops.png", caption="Powered by Xatu")
st.sidebar.title("Xatu Dashboards")

# Create dashboard links
dashboard_options = ["Home"] + list(DASHBOARDS.keys())
for dashboard in dashboard_options:
    icon = "🏠" if dashboard == "Home" else DASHBOARDS.get(dashboard, {}).get("icon", "")
    is_active = st.session_state.current_dashboard == dashboard
    
    # Style active dashboard differently
    prefix = "▶ " if is_active else ""
    label = f"{prefix}{icon} {dashboard}"
    
    # Create the dashboard link
    if st.sidebar.button(label, key=f"nav_{dashboard}", 
                       use_container_width=True,
                       type="primary" if is_active else "secondary"):
        st.session_state.current_dashboard = dashboard
        dashboard_url = get_dashboard_url(dashboard)
        # Update URL params
        params_dict = dict(urllib.parse.parse_qsl(dashboard_url[1:]))
        for key, value in params_dict.items():
            st.query_params[key] = value
        st.rerun()

# Add global settings in sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("Global Settings")

# Network selection
network_index = SUPPORTED_NETWORKS.index(st.session_state.network)
selected_network = st.sidebar.selectbox(
    "Network", 
    SUPPORTED_NETWORKS,
    index=network_index
)

# Update session state and URL if network changed
if selected_network != st.session_state.network:
    st.session_state.network = selected_network
    # Update URL params
    st.query_params["network"] = selected_network
    st.query_params["dashboard"] = st.session_state.current_dashboard.lower().replace(" ", "-") if st.session_state.current_dashboard != "Home" else "home"
    st.query_params["time_window"] = st.session_state.time_window
    st.rerun()

# Time window selection
time_window_index = list(TIME_WINDOWS.keys()).index(st.session_state.time_window)
selected_time_window = st.sidebar.selectbox(
    "Time window", 
    list(TIME_WINDOWS.keys()),
    index=time_window_index
)

# Update session state and URL if time window changed
if selected_time_window != st.session_state.time_window:
    st.session_state.time_window = selected_time_window
    # Update URL params
    st.query_params["time_window"] = selected_time_window
    st.query_params["dashboard"] = st.session_state.current_dashboard.lower().replace(" ", "-") if st.session_state.current_dashboard != "Home" else "home"
    st.query_params["network"] = st.session_state.network
    st.rerun()

# Force refresh option
st.session_state.force_refresh = st.sidebar.checkbox(
    "Force refresh data", 
    value=st.session_state.force_refresh,
    help="Force refresh cached data (for development)"
)

# Display version info
st.sidebar.markdown("---")
st.sidebar.markdown("*Xatu Dashboard v0.1*")

# Render the selected dashboard
current_dashboard = st.session_state.current_dashboard
if current_dashboard == "Home":
    # Dashboard cards section
    st.markdown("## Available Dashboards")
    
    # Custom CSS for better cards
    st.markdown("""
    <style>
    .dashboard-card {
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin: 10px 0;
        background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        height: 100%;
    }
    .dashboard-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 20px rgba(0, 0, 0, 0.15);
    }
    .dashboard-card h3 {
        color: #1E88E5;
        margin-bottom: 10px;
    }
    .dashboard-card p {
        color: #616161;
        margin-bottom: 15px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create a grid of dashboard cards
    cols = st.columns(3)
    
    # Add a card for each dashboard
    for i, (name, config) in enumerate(DASHBOARDS.items()):
        with cols[i % 3]:
            st.markdown(f"""
            <div class="dashboard-card">
                <h3>{config.get('icon', '')} {name}</h3>
                <p>{config.get('description', '')}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Add button below each card
            if st.button(f"View {name}", key=f"btn_{config['module']}", use_container_width=True):
                st.session_state.current_dashboard = name
                # Update URL params
                dashboard_slug = name.lower().replace(" ", "-")
                st.query_params["dashboard"] = dashboard_slug
                st.query_params["network"] = st.session_state.network
                st.query_params["time_window"] = st.session_state.time_window
                st.rerun()
    
    # Add more information at the bottom
    st.markdown("---")
    st.markdown("""
    ### About Xatu Dashboard
    
    This dashboard provides insights into Ethereum network data collected by Xatu. 
    Use the sidebar to navigate between dashboards and adjust settings.
    """)
else:
    # Load and render the selected dashboard
    dashboard_config = DASHBOARDS.get(current_dashboard)
    if dashboard_config:
        try:
            # Import the dashboard module dynamically
            module_name = f"dashboards.{dashboard_config['module']}"
            dashboard_module = importlib.import_module(module_name)
            
            # Call the render function with global settings
            dashboard_module.render(
                force_refresh=st.session_state.force_refresh
            )
        except Exception as e:
            st.error(f"Error loading dashboard: {str(e)}")
            st.exception(e) 