from dashboards.block_arrival import render

# Re-export the render function for backward compatibility
# This makes it transparent to consumers whether they're using
# the module or the package version

if __name__ == "__main__":
    import streamlit as st
    from config import SUPPORTED_NETWORKS, TIME_WINDOWS
    import urllib.parse
    
    # Get URL parameters
    query_params = st.query_params
    
    # Initialize session state for standalone mode
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
            st.session_state.time_window = list(TIME_WINDOWS.keys())[0]
    
    # Function to update URL parameters
    def update_url_params():
        st.query_params["network"] = st.session_state.network
        st.query_params["time_window"] = st.session_state.time_window
    
    # Add a sidebar toggle for force refresh in standalone mode
    st.sidebar.title("Dashboard Settings")
    
    # Network selection
    network_index = SUPPORTED_NETWORKS.index(st.session_state.network)
    selected_network = st.sidebar.selectbox("Network", SUPPORTED_NETWORKS, index=network_index)
    if selected_network != st.session_state.network:
        st.session_state.network = selected_network
        update_url_params()
        st.rerun()
    
    # Time window selection
    time_window_index = list(TIME_WINDOWS.keys()).index(st.session_state.time_window)
    selected_time_window = st.sidebar.selectbox("Time window", list(TIME_WINDOWS.keys()), index=time_window_index)
    if selected_time_window != st.session_state.time_window:
        st.session_state.time_window = selected_time_window
        update_url_params()
        st.rerun()
    
    # Force refresh option
    st.session_state.force_refresh = st.sidebar.checkbox("Force refresh data", value=st.session_state.force_refresh)
    
    # Render dashboard
    render(force_refresh=st.session_state.force_refresh) 