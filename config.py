# Supported networks
SUPPORTED_NETWORKS = ["mainnet", "holesky", "sepolia"]

# Refresh time in seconds (3 hours)
DEFAULT_REFRESH_TIME = 3 * 60 * 60

# Supported time windows as key-value pairs (display name: days)
TIME_WINDOWS = {
    "1 day": 1,
    "7 days": 7,
    "31 days": 31,
    "90 days": 90
}

# Default time window index for selectbox
DEFAULT_TIME_WINDOW_INDEX = 0

# Dashboard configuration
DASHBOARDS = {
    "Block Arrival Times": {
        "module": "block_arrival",
        "icon": "📦",
        "description": "Analyze block propagation times across different networks and clients"
    },
    "Xatu User Deep Dive": {
        "module": "user_deep_dive",
        "icon": "👤",
        "description": "Analyze data from individual Xatu users and their nodes"
    },
    "Node Deep Dive": {
        "module": "node_deep_dive",
        "icon": "🖥️",
        "description": "Detailed metrics and performance data for individual nodes"
    }
}

# Xatu API base URL
XATU_BASE_URL = "https://data.ethpandaops.io/xatu"

# Database name (usually "default")
DATABASE = "default" 