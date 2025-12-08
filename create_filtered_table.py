import duckdb
import pandas as pd

def create_filtered_options_table():
    # Define the symbols to filter by
    symbols_to_keep = ['SPX', 'SPXW', 'QQQ', 'SPY']
    
    # Connect to the DuckDB database
    conn = duckdb.connect('option_data.duckdb')
    
    try:
        # Read the CSV file in chunks to handle large files efficiently
        chunk_size = 100000
        filtered_chunks = []
        
        print("Reading and filtering CSV data...")
        for chunk in pd.read_csv('Greek_20231227_OData2.csv', chunksize=chunk_size, low_memory=False):
            # Filter for the desired symbols
            filtered = chunk[chunk['Symbol'].isin(symbols_to_keep)]
            if not filtered.empty:
                filtered_chunks.append(filtered)
            print(f"Processed {len(chunk)} rows, found {len(filtered)} matching rows")
        
        if not filtered_chunks:
            print("No data found for the specified symbols.")
            return
        
        # Combine all filtered chunks
        filtered_df = pd.concat(filtered_chunks, ignore_index=True)
        print(f"Total matching rows found: {len(filtered_df)}")
        
        # Write to DuckDB
        print("Writing to DuckDB...")
        conn.register('temp_df', filtered_df)
        conn.execute("""
            CREATE OR REPLACE TABLE optionData_discountOption_data AS
            SELECT * FROM temp_df
        """)
        
        # Verify the table was created
        result = conn.execute("""
            SELECT COUNT(*) as row_count 
            FROM optionData_discountOption_data
        """).fetchone()
        
        print(f"\nSuccessfully created table 'optionData_discountOption_data' with {result[0]} rows")
        
        # Show table schema
        print("\nTable schema:")
        print(conn.execute("DESCRIBE optionData_discountOption_data").fetchall())
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    create_filtered_options_table()
