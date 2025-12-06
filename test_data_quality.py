#!/usr/bin/env python3
"""
Data Quality Validation Script for Option Data
Tests bid/ask values, timestamp intervals, and delta ranges
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime, time
from process_parquet_files import process_option_data

def test_data_quality(file_path):
    """Test data quality for a single parquet file."""
    print(f"\n=== Testing {file_path.name} ===")
    
    # Read and process the file
    df = pd.read_parquet(file_path)
    df['ticker'] = file_path.parent.name
    df['date'] = pd.to_datetime(file_path.stem.split('_')[-1], format='%Y%m%d')
    
    # Process the response column
    all_flattened_rows = []
    for _, row in df.iterrows():
        option_data = row['response']
        if isinstance(option_data, str):
            option_data = json.loads(option_data)
        
        flattened_options = process_option_data(option_data)
        for option in flattened_options:
            if 'data' in option and isinstance(option['data'], (list, np.ndarray)):
                data_array = option['data']
                if isinstance(data_array, np.ndarray):
                    data_array = data_array.tolist()
                
                for data_point in data_array:
                    new_row = {}
                    for col in df.columns:
                        if col != 'response':
                            new_row[col] = row[col]
                    
                    contract_info = {k: v for k, v in option.items() if k != 'data'}
                    new_row.update(contract_info)
                    
                    if isinstance(data_point, dict):
                        for k, v in data_point.items():
                            if k != 'implied_vol':
                                new_row[f'data_{k}'] = v
                    
                    all_flattened_rows.append(new_row)
    
    df_flat = pd.DataFrame(all_flattened_rows)
    df_flat.columns = [col.rstrip('_') for col in df_flat.columns]
    
    # Convert timestamps
    df_flat['data_timestamp'] = pd.to_datetime(df_flat['data_timestamp'])
    
    # Test 1: Bid/Ask values
    print(f"1. Bid/Ask Values Test:")
    bid_zero_count = (df_flat['data_bid'] == 0).sum()
    ask_zero_count = (df_flat['data_ask'] == 0).sum()
    total_rows = len(df_flat)
    
    print(f"   Total rows: {total_rows:,}")
    print(f"   Rows with bid = 0: {bid_zero_count:,} ({bid_zero_count/total_rows*100:.1f}%)")
    print(f"   Rows with ask = 0: {ask_zero_count:,} ({ask_zero_count/total_rows*100:.1f}%)")
    print(f"   Rows with bid > 0: {(df_flat['data_bid'] > 0).sum():,}")
    print(f"   Rows with ask > 0: {(df_flat['data_ask'] > 0).sum():,}")
    
    # Test 2: Timestamp intervals and range
    print(f"\n2. Timestamp Test:")
    timestamps = df_flat['data_timestamp']
    min_time = timestamps.min().time()
    max_time = timestamps.max().time()
    unique_dates = timestamps.dt.date.nunique()
    
    print(f"   Date range: {timestamps.min().date()} to {timestamps.max().date()}")
    print(f"   Time range: {min_time} to {max_time}")
    print(f"   Unique dates: {unique_dates}")
    
    # Check 1-minute intervals for each day
    interval_issues = 0
    for date in timestamps.dt.date.unique():
        day_data = timestamps[timestamps.dt.date == date].sort_values()
        time_diffs = day_data.diff().dt.total_seconds().dropna()
        # Allow for some missing intervals (market breaks, etc.)
        expected_intervals = set([60] * (len(time_diffs) - 1))  # 1 minute = 60 seconds
        actual_intervals = set(time_diffs.astype(int))
        
        if not actual_intervals.issubset({60, 3600, 7200}):  # 1min, 1hr, 2hr breaks
            interval_issues += 1
    
    print(f"   Days with proper 1-minute intervals: {unique_dates - interval_issues}/{unique_dates}")
    
    # Test 3: Delta values
    print(f"\n3. Delta Values Test:")
    delta_stats = df_flat['data_delta'].describe()
    delta_between_01_02 = ((df_flat['data_delta'] >= 0.1) & (df_flat['data_delta'] <= 0.2)).sum()
    delta_negative = (df_flat['data_delta'] < 0).sum()
    delta_positive = (df_flat['data_delta'] > 0).sum()
    delta_zero = (df_flat['data_delta'] == 0).sum()
    
    print(f"   Delta min: {delta_stats['min']:.4f}")
    print(f"   Delta max: {delta_stats['max']:.4f}")
    print(f"   Delta mean: {delta_stats['mean']:.4f}")
    print(f"   Values between 0.1-0.2: {delta_between_01_02:,} ({delta_between_01_02/total_rows*100:.1f}%)")
    print(f"   Negative values: {delta_negative:,} ({delta_negative/total_rows*100:.1f}%)")
    print(f"   Positive values: {delta_positive:,} ({delta_positive/total_rows*100:.1f}%)")
    print(f"   Zero values: {delta_zero:,} ({delta_zero/total_rows*100:.1f}%)")
    
    # Test 4: Sample data inspection
    print(f"\n4. Sample Data (first 5 rows):")
    sample = df_flat[['contract_strike', 'contract_right', 'data_bid', 'data_ask', 'data_delta', 'data_timestamp']].head()
    print(sample.to_string(index=False))
    
    return {
        'file': file_path.name,
        'total_rows': total_rows,
        'bid_zero_pct': bid_zero_count/total_rows*100,
        'ask_zero_pct': ask_zero_count/total_rows*100,
        'time_range': (min_time, max_time),
        'delta_mean': delta_stats['mean'],
        'delta_between_01_02_pct': delta_between_01_02/total_rows*100
    }

def main():
    """Run data quality tests on sample files."""
    from pathlib import Path
    
    # Test a few sample files
    test_files = [
        Path('data/SPXW/SPXW_20251201.parquet'),
        Path('data/SPY/SPY_20240701.parquet'),
    ]
    
    results = []
    
    for file_path in test_files:
        if file_path.exists():
            result = test_data_quality(file_path)
            results.append(result)
        else:
            print(f"File not found: {file_path}")
    
    # Summary
    print(f"\n=== SUMMARY ===")
    for result in results:
        print(f"\n{result['file']}:")
        print(f"  Rows: {result['total_rows']:,}")
        print(f"  Bid zeros: {result['bid_zero_pct']:.1f}%")
        print(f"  Ask zeros: {result['ask_zero_pct']:.1f}%")
        print(f"  Time range: {result['time_range'][0]} - {result['time_range'][1]}")
        print(f"  Delta mean: {result['delta_mean']:.4f}")
        print(f"  Delta 0.1-0.2: {result['delta_between_01_02_pct']:.1f}%")

if __name__ == "__main__":
    main()
