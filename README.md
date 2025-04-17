# Xatu Dashboard

A Streamlit-based dashboard for visualizing Ethereum network data from Xatu.

## Features

- Multiple dashboards for different network metrics
- Support for multiple Ethereum networks (mainnet, holesky, sepolia)
- Data caching for faster loading
- Configurable time windows and refresh intervals
- Interactive visualizations

## Currently Available Dashboards

- **Block Arrival Times**: Analyze block propagation times across different networks with percentiles and hourly trends

## Installation

```bash
# Clone the repository
git clone https://github.com/samcm/xatu-dashboard.git
cd xatu-dashboard

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Run the dashboard
streamlit run app.py
```

## Development

- Dashboard modules are located in the `dashboards/` directory
- Configuration settings in `config.py`
- Data loading utilities in `utils.py`

## License

MIT 