import duckdb
import pandas as pd
from datetime import datetime

def compare_tables():
    # Connect to the DuckDB database
    conn = duckdb.connect('option_data.duckdb')
    
    # Query to find matching records between the two tables
    query = """
    WITH 
    -- Get common tickers and dates
    common_dates AS (
        SELECT DISTINCT 
            b.symbol,
            b.trade_date,
            b.data_timestamp::time as trade_time
        FROM optionData_Backtesting b
        JOIN optionData_discountOption_data d
            ON b.symbol = d.Symbol
            AND b.trade_date = d.DataDate::date
            AND b.data_timestamp::time = d.DataDate::time
    )
    
    -- Get matching records with all required fields
    SELECT 
        b.symbol,
        b.trade_date,
        b.data_timestamp,
        b.contract_strike,
        b.contract_right,
        b.data_delta,
        b.data_bid as backtest_bid,
        b.data_ask as backtest_ask,
        d.BidPrice as discount_bid,
        d.AskPrice as discount_ask,
        d.Delta as discount_delta,
        (b.data_bid - d.BidPrice) as bid_difference,
        (b.data_ask - d.AskPrice) as ask_difference,
        (b.data_delta - d.Delta) as delta_difference
    FROM optionData_Backtesting b
    JOIN optionData_discountOption_data d
        ON b.symbol = d.Symbol
        AND b.trade_date = d.DataDate::date
        AND b.data_timestamp::time = d.DataDate::time
        AND b.contract_strike = d.StrikePrice
        AND (
            (b.contract_right = 'C' AND d.PutCall = 'call') OR 
            (b.contract_right = 'P' AND d.PutCall = 'put')
        )
    ORDER BY 
        b.trade_date DESC,
        b.data_timestamp DESC,
        b.symbol,
        b.contract_strike,
        b.contract_right
    """
    
    # Execute the query and get results as a DataFrame
    df = conn.execute(query).fetchdf()
    
    # Save results to a CSV file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'comparison_results_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    print(f"Comparison complete! Results saved to {output_file}")
    print(f"Total matching records found: {len(df)}")
    
    # Show summary statistics
    if not df.empty:
        print("\nSummary Statistics:")
        print("Bid Price Differences (Backtest - Discount):")
        print(df['bid_difference'].describe())
        print("\nAsk Price Differences (Backtest - Discount):")
        print(df['ask_difference'].describe())
        print("\nDelta Differences (Backtest - Discount):")
        print(df['delta_difference'].describe())
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    compare_tables()
