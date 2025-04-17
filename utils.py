import os
import requests
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import logging
from config import XATU_BASE_URL, DATABASE

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xatu-dashboard")

def get_xatu_tables():
    """Get mapping of Xatu tables to their URLs"""
    url = "https://raw.githubusercontent.com/ethpandaops/xatu-data/master/llms.txt"
    resp = requests.get(url)
    
    mapping = {}
    for line in resp.text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) == 2:
            table, url = parts
            mapping[table] = url
    
    return mapping

def get_parquet_url(network, table, date):
    """Construct URL for a parquet file based on table partitioning"""
    if date is None:
        raise ValueError("Date must be provided to get_parquet_url")
    
    # According to the documentation, URL format is:
    # https://data.ethpandaops.io/xatu/NETWORK/databases/DATABASE/TABLE/YYYY/MM/DD.parquet
    return f"{XATU_BASE_URL}/{network}/databases/{DATABASE}/{table}/{date.year}/{date.month}/{date.day}.parquet"

def load_xatu_data(network, table, date, use_cache=True):
    """Load data from Xatu parquet files with caching support"""
    if date is None:
        raise ValueError("Date must be provided to load_xatu_data")
    
    # Create cache directory if it doesn't exist
    cache_dir = Path("data")
    cache_dir.mkdir(exist_ok=True)
    
    # Cache file path
    cache_file = cache_dir / f"{network}_{table}_{date.isoformat()}.parquet"
    
    # Check if cached file exists
    if use_cache and cache_file.exists():
        logger.info(f"Using cached data for {network}_{table}")
        try:
            df = pl.read_parquet(cache_file)
            logger.info(f"Read DataFrame type: {type(df)}")
            if df is not None and len(df) > 0:
                return df
            # If reading succeeds but dataframe is empty, continue to download
        except Exception as e:
            logger.error(f"Error reading cached file: {str(e)}")
            # If cache is corrupted, delete it and continue to download
            cache_file.unlink(missing_ok=True)
    
    # Construct URL for the parquet file
    url = get_parquet_url(network, table, date)
    logger.info(f"Downloading from: {url}")
    
    # Download the parquet file
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Save to cache
        with open(cache_file, "wb") as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded data for {network}_{table}")
        
        # Read the parquet file
        try:
            df = pl.read_parquet(cache_file)
            logger.info(f"Read DataFrame type after download: {type(df)}")
            logger.info(f"DataFrame columns: {df.columns}")
            
            if df is not None and len(df) > 0:
                return df
            else:
                logger.warning(f"Parquet file downloaded but is empty: {cache_file}")
                return None
        except Exception as e:
            logger.error(f"Error reading downloaded parquet: {str(e)}")
            cache_file.unlink(missing_ok=True)
            return None
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Data not found for {network}_{table} on {date.isoformat()}")
            return None
        else:
            logger.error(f"HTTP error for {network}_{table}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error downloading {network}_{table}: {str(e)}")
        return None

def load_xatu_data_range(network, table, start_date, end_date, use_cache=True, progress_callback=None):
    """Load data from Xatu parquet files for a date range
    
    Args:
        network: Network name
        table: Table name
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        use_cache: Whether to use cached data
        progress_callback: Optional callback function to update progress
        
    Returns:
        Combined polars DataFrame or None if no data is available
    """
    if start_date is None or end_date is None:
        raise ValueError("Start and end dates must be provided")
    
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")
    
    dfs = []
    total_days = (end_date - start_date).days + 1
    days_processed = 0
    
    # Iterate through each day in the range
    current_date = start_date
    while current_date <= end_date:
        if progress_callback:
            progress_callback(days_processed / total_days)
        
        logger.info(f"Loading data for {current_date.isoformat()}")
        df = load_xatu_data(network, table, current_date, use_cache=use_cache)
        
        if df is not None and len(df) > 0:
            dfs.append(df)
        
        current_date += timedelta(days=1)
        days_processed += 1
    
    # Combine all dataframes
    if dfs:
        try:
            combined_df = pl.concat(dfs)
            logger.info(f"Combined data: {len(combined_df)} rows")
            return combined_df
        except Exception as e:
            logger.error(f"Error combining dataframes: {str(e)}")
            return None
    else:
        logger.warning(f"No data available for the date range {start_date.isoformat()} to {end_date.isoformat()}")
        return None

def format_ms(ms):
    """Format milliseconds for display"""
    return f"{ms:.2f}ms" 