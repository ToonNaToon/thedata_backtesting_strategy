import duckdb
import pandas as pd
from datetime import datetime

def diagnose_tables():
    # Connect to the DuckDB database
    conn = duckdb.connect('option_data.duckdb')
    
    # 1. Check date ranges and common dates
    print("=== Date Range Analysis ===")
    
    # Check date ranges in both tables
    backtest_dates = conn.execute("""
        SELECT 
            MIN(trade_date) as min_date,
            MAX(trade_date) as max_date,
            COUNT(DISTINCT trade_date) as unique_dates
        FROM optionData_Backtesting
    """).fetchone()
    
    discount_dates = conn.execute("""
        SELECT 
            MIN(DataDate::date) as min_date,
            MAX(DataDate::date) as max_date,
            COUNT(DISTINCT DataDate::date) as unique_dates
        FROM optionData_discountOption_data
    """).fetchone()
    
    print("\nBacktesting Table Date Range:")
    print(f"From: {backtest_dates[0]} to {backtest_dates[1]}")
    print(f"Unique dates: {backtest_dates[2]}")
    
    print("\nDiscount Option Data Table Date Range:")
    print(f"From: {discount_dates[0]} to {discount_dates[1]}")
    print(f"Unique dates: {discount_dates[2]}")
    
    # 2. Check for common dates
    common_dates = conn.execute("""
        SELECT DISTINCT b.trade_date
        FROM optionData_Backtesting b
        JOIN optionData_discountOption_data d
            ON b.trade_date = d.DataDate::date
        LIMIT 10
    """).fetchall()
    
    print(f"\nSample of common dates (up to 10): {[d[0] for d in common_dates]}")
    
    # 3. Check symbol matching
    print("\n=== Symbol Analysis ===")
    backtest_symbols = conn.execute("SELECT DISTINCT symbol FROM optionData_Backtesting").fetchall()
    discount_symbols = conn.execute("SELECT DISTINCT Symbol FROM optionData_discountOption_data").fetchall()
    
    print("\nUnique symbols in Backtesting table:")
    print([s[0] for s in backtest_symbols])
    print("\nUnique symbols in Discount Option Data table:")
    print([s[0] for s in discount_symbols])
    
    # 4. Check time format differences
    print("\n=== Time Format Analysis ===")
    backtest_times = conn.execute("""
        SELECT DISTINCT data_timestamp::time as time_only
        FROM optionData_Backtesting
        ORDER BY time_only
        LIMIT 5
    """).fetchall()
    
    discount_times = conn.execute("""
        SELECT DISTINCT DataDate::time as time_only
        FROM optionData_discountOption_data
        ORDER BY time_only
        LIMIT 5
    """).fetchall()
    
    print("\nSample times in Backtesting table:")
    print([str(t[0]) for t in backtest_times])
    print("\nSample times in Discount Option Data table:")
    print([str(t[0]) for t in discount_times])
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    diagnose_tables()
