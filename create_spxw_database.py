#!/usr/bin/env python3
"""
Create a DuckDB database with only SPXW data and compress it to .gz
"""

import os
import pandas as pd
import json
import numpy as np
from pathlib import Path
import duckdb
import logging
import gzip
import shutil
from datetime import datetime

# Configure logging
log_filename = f"spxw_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_duckdb_connection(db_path):
    """Create a DuckDB connection."""
    try:
        connection = duckdb.connect(db_path)
        logger.info(f"Successfully connected to DuckDB database: {db_path}")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        return None

def create_table_if_not_exists(connection):
    """Create the optionData_Backtesting table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS optionData_Backtesting (
        symbol VARCHAR,
        trade_date DATE,
        ticker VARCHAR,
        date DATE,
        contract_expiration DATE,
        contract_right VARCHAR,
        contract_strike DECIMAL,
        contract_symbol VARCHAR,
        data_ask DECIMAL,
        data_bid DECIMAL,
        data_delta DECIMAL,
        data_epsilon DECIMAL,
        data_iv_error DECIMAL,
        data_lambda DECIMAL,
        data_rho DECIMAL,
        data_theta DECIMAL,
        data_timestamp TIMESTAMP,
        data_underlying_price DECIMAL,
        data_underlying_timestamp TIMESTAMP,
        data_vega DECIMAL
    )
    """
    
    try:
        connection.execute(create_table_query)
        logger.info("Table optionData_Backtesting created or already exists")
    except Exception as e:
        logger.error(f"Error creating table: {e}")

def insert_data_to_duckdb(connection, df):
    """Insert DataFrame data into DuckDB table."""
    try:
        # Get the actual column order from the DataFrame
        columns = df.columns.tolist()
        
        # Build explicit INSERT statement with column names
        columns_str = ', '.join(columns)
        insert_query = f"INSERT INTO optionData_Backtesting ({columns_str}) SELECT {columns_str} FROM df"
        
        # Use DuckDB's efficient bulk insert with explicit column mapping
        connection.execute(insert_query)
        rows_inserted = len(df)
        logger.info(f"Successfully inserted {rows_inserted} rows into optionData_Backtesting")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        return 0

def process_option_data(option_data):
    """
    Process option data which could be a dictionary, list, or numpy array.
    Returns a list of flattened option dictionaries.
    """
    import numpy as np
    
    def flatten_dict(d, prefix=''):
        """Flatten a nested dictionary."""
        items = {}
        for k, v in d.items():
            new_key = f"{prefix}{k}" if prefix else k
            if isinstance(v, dict):
                items.update(flatten_dict(v, f"{new_key}_"))
            else:
                items[new_key] = v
        return items
    
    try:
        # Handle different data types
        if isinstance(option_data, dict):
            # If it's a dictionary with 'data' key, flatten it
            if 'data' in option_data:
                return [flatten_dict(option_data)]
            else:
                return [option_data]
        elif isinstance(option_data, (list, np.ndarray)):
            # If it's a list/array, process each element
            result = []
            for item in option_data:
                if isinstance(item, dict):
                    result.append(flatten_dict(item))
                else:
                    result.append({'value': item})
            return result
        else:
            # If it's a single value
            return [{'value': option_data}]
            
    except Exception as e:
        logger.error(f"Error processing option data: {e}")
        return []

def find_spxw_parquet_files(data_dir):
    """Find all parquet files in SPXW subdirectories."""
    data_path = Path(data_dir)
    spxw_files = []
    
    # Look for SPXW directory and its subdirectories
    spxw_path = data_path / "SPXW"
    if spxw_path.exists():
        # Find all parquet files in SPXW directory and subdirectories
        for parquet_file in spxw_path.rglob("*.parquet"):
            spxw_files.append(parquet_file)
    
    return spxw_files

def process_spxw_files(data_dir, output_db_path="option_data_spxw.duckdb"):
    """Process only SPXW parquet files and create a new DuckDB database."""
    
    # Find SPXW parquet files
    parquet_files = find_spxw_parquet_files(data_dir)
    
    if not parquet_files:
        logger.error(f"No SPXW parquet files found in {data_dir}")
        return 0
    
    logger.info(f"Found {len(parquet_files)} SPXW parquet files to process")
    
    # Create DuckDB connection
    connection = create_duckdb_connection(output_db_path)
    if not connection:
        return 0
    
    # Create table if it doesn't exist
    create_table_if_not_exists(connection)
    
    total_rows_inserted = 0
    files_processed = 0
    
    try:
        for file_path in parquet_files:
            logger.info(f"Processing SPXW file: {file_path}")
            
            try:
                # Read the parquet file
                df = pd.read_parquet(file_path)
                
                # Set ticker to SPXW
                df['ticker'] = 'SPXW'
                
                # Add a column for the date (extracted from filename if possible)
                date_str = file_path.stem.split('_')[-1]
                try:
                    df['date'] = pd.to_datetime(date_str, format='%Y%m%d')
                except:
                    logger.warning(f"Could not extract date from filename: {file_path.name}")
                    df['date'] = pd.NaT
                
                # Process the response column to flatten the JSON
                if 'response' in df.columns:
                    # Create a list to hold all flattened rows
                    all_flattened_rows = []
                    
                    # Process each row in the original dataframe
                    for _, row in df.iterrows():
                        # Process the response data
                        option_data = row['response']
                        
                        # Convert string to Python object if needed
                        if isinstance(option_data, str):
                            try:
                                option_data = json.loads(option_data)
                            except json.JSONDecodeError:
                                logger.warning(f"Warning: Could not parse response as JSON: {option_data[:100]}...")
                                continue
                        
                        # Process the option data
                        try:
                            flattened_options = process_option_data(option_data)
                            
                            # Add the original row data to each flattened option
                            for option in flattened_options:
                                # Check if this option has data array
                                if 'data' in option and isinstance(option['data'], (list, np.ndarray)):
                                    data_array = option['data']
                                    if isinstance(data_array, np.ndarray):
                                        data_array = data_array.tolist()
                                    
                                    # Create a separate row for each data point (timestamp)
                                    for data_point in data_array:
                                        new_row = {}
                                        # Add all the original columns except response
                                        for col in df.columns:
                                            if col != 'response':
                                                new_row[col] = row[col]
                                        
                                        # Add the flattened contract info (excluding the data array)
                                        contract_info = {k: v for k, v in option.items() if k != 'data'}
                                        new_row.update(contract_info)
                                        
                                        # Add the individual data point fields
                                        if isinstance(data_point, dict):
                                            for k, v in data_point.items():
                                                # Skip implied_vol to avoid data range errors
                                                if k != 'implied_vol':
                                                    new_row[f"data_{k}"] = v
                                        
                                        all_flattened_rows.append(new_row)
                                else:
                                    # No data array, just add the contract as a single row
                                    new_row = {}
                                    for col in df.columns:
                                        if col != 'response':
                                            new_row[col] = row[col]
                                    new_row.update(option)
                                    all_flattened_rows.append(new_row)
                                        
                        except Exception as e:
                            logger.error(f"Error processing row in {file_path}: {e}")
                            continue
                    
                    if not all_flattened_rows:
                        logger.warning(f"Warning: No valid data was processed from {file_path}")
                        continue
                        
                    # Create a new dataframe with all flattened rows
                    df_flat = pd.DataFrame(all_flattened_rows)
                    
                    # Clean up column names (remove any trailing underscores)
                    df_flat.columns = [col.rstrip('_') for col in df_flat.columns]
                    
                    # Insert data into DuckDB
                    rows_inserted = insert_data_to_duckdb(connection, df_flat)
                    if rows_inserted > 0:
                        total_rows_inserted += rows_inserted
                        files_processed += 1
                        logger.info(f"Successfully processed {file_path.name}: {rows_inserted} rows inserted")
                    else:
                        logger.error(f"Failed to insert data from {file_path.name}")
                else:
                    logger.warning(f"No 'response' column found in {file_path}")
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
    
    finally:
        connection.close()
        logger.info("DuckDB connection closed")
    
    logger.info(f"\nSPXW processing complete!")
    logger.info(f"Files processed: {files_processed}/{len(parquet_files)}")
    logger.info(f"Total rows inserted: {total_rows_inserted}")
    
    return total_rows_inserted

def compress_database(db_path, remove_original=False):
    """Compress the DuckDB database to .gz format."""
    gz_path = f"{db_path}.gz"
    
    try:
        logger.info(f"Compressing {db_path} to {gz_path}...")
        
        with open(db_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Get file sizes
        original_size = os.path.getsize(db_path)
        compressed_size = os.path.getsize(gz_path)
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        logger.info(f"Compression complete!")
        logger.info(f"Original size: {original_size/1024/1024:.1f}MB")
        logger.info(f"Compressed size: {compressed_size/1024/1024:.1f}MB")
        logger.info(f"Compression ratio: {compression_ratio:.1f}%")
        
        if remove_original:
            os.remove(db_path)
            logger.info(f"Removed original file: {db_path}")
        
        return gz_path
        
    except Exception as e:
        logger.error(f"Error compressing database: {e}")
        return None

if __name__ == "__main__":
    data_dir = "data"
    output_db = "option_data_spxw.duckdb"
    
    # Process only SPXW parquet files
    total_rows = process_spxw_files(data_dir, output_db)
    
    if total_rows > 0:
        print(f"\nSuccess! Total SPXW rows inserted: {total_rows}")
        print(f"Log file saved as: {log_filename}")
        print(f"DuckDB database created: {output_db}")
        
        # Compress the database
        compressed_file = compress_database(output_db, remove_original=False)
        if compressed_file:
            print(f"Compressed database created: {compressed_file}")
    else:
        print("\nNo SPXW data was processed. Please check the data directory and log file.")
        print(f"Log file: {log_filename}")
