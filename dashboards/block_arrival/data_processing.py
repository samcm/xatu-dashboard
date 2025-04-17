import polars as pl
import pandas as pd
import logging
import streamlit as st

# Set up logging
logger = logging.getLogger("xatu-dashboard")

def preprocess_data(df, network):
    """Preprocess raw data for the block arrival dashboard
    
    This function handles:
    1. Converting between pandas/polars formats
    2. Converting binary columns to string
    3. Capping propagation times
    4. Adding derived metrics
    5. Handling multiple entries per block
    """
    # Debug DataFrame type
    logger.info(f"preprocess_data: df type = {type(df)}")
    
    try:
        # Convert to polars if it's pandas
        if isinstance(df, pd.DataFrame):
            logger.info("Converting pandas DataFrame to polars")
            df = pl.from_pandas(df)
        # Ensure we're working with a polars DataFrame
        elif not isinstance(df, pl.DataFrame):
            error_msg = f"Unsupported DataFrame type: {type(df)}"
            logger.error(error_msg)
            st.error(error_msg)
            return None
        
        # Convert binary columns to string for better analysis
        # First, identify binary columns that need conversion
        binary_cols = [
            "meta_client_implementation", "meta_client_name", "meta_client_version",
            "meta_client_geo_country", "meta_client_geo_country_code", "meta_client_geo_city",
            "meta_network_name", "meta_consensus_implementation", "meta_consensus_version"
        ]
        
        # Convert columns
        for col in df.columns:
            if df.schema[col] in [pl.Binary, pl.UInt32]:
                if col in binary_cols:
                    df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
        
        # Cap propagation times at 6000ms
        df = df.with_columns(
            pl.when(pl.col("propagation_slot_start_diff") > 6000)
              .then(6000)
              .otherwise(pl.col("propagation_slot_start_diff"))
              .alias("capped_diff_ms")
        )
        
        # Add network column
        df = df.with_columns(pl.lit(network).alias("network"))
        
        # Extract hour from event_date_time for time analysis
        df = df.with_columns(
            pl.col("event_date_time").dt.hour().alias("hour")
        )
        
        # Create unique block identifier for aggregation
        # This helps us handle multiple entries per block from different clients
        df = df.with_columns(
            (pl.col("slot").cast(pl.Utf8) + "_" + pl.col("epoch").cast(pl.Utf8)).alias("block_id")
        )
        
        # Create block-level metrics
        # These will be used to analyze block arrival per block, not per observation
        block_stats = df.group_by("block_id").agg(
            pl.col("slot").first().alias("slot"),
            pl.col("epoch").first().alias("epoch"),
            pl.col("event_date_time").min().alias("first_seen_time"),
            pl.col("capped_diff_ms").min().alias("min_propagation_ms"),
            pl.col("capped_diff_ms").mean().alias("mean_propagation_ms"),
            pl.col("capped_diff_ms").median().alias("median_propagation_ms"),
            pl.col("capped_diff_ms").quantile(0.9).alias("p90_propagation_ms"),
            pl.count().alias("num_observations"),
            pl.col("hour").first().alias("hour")
        )
        
        # For client analysis, we'll keep the original data with client information
        # Add a flag for slow propagation to help identify problematic clients
        df = df.with_columns(
            pl.when(pl.col("capped_diff_ms") > df.select(pl.col("capped_diff_ms").quantile(0.75))[0, 0])
              .then(True)
              .otherwise(False)
              .alias("is_slow_propagation")
        )
        
        # Return both dataframes in a dictionary
        return {
            "raw": df,  # Original data with client information
            "block_stats": block_stats,  # Aggregated block-level statistics
            "network": network  # Add network for reference
        }
    
    except Exception as e:
        error_msg = f"Error in preprocess_data: {str(e)}"
        logger.error(error_msg)
        st.error(error_msg)
        return None 