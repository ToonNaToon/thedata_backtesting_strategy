#!/usr/bin/env python3
"""
0DTE Iron Condor Backtesting Script for SPXW
Implements the iron condor strategy with delta-based strike selection
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Dict, List, Tuple, Optional
import logging
import os
import requests
import gzip
import shutil
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_database(db_path: str = "option_data.duckdb", force_download: bool = False):
    """
    Download and extract the database file from Google Drive if it doesn't exist.
    
    Args:
        db_path: Path where the database file should be located
        force_download: Force download even if file exists
    """
    if os.path.exists(db_path) and not force_download:
        logger.info(f"Database file already exists: {db_path}")
        return True
    
    # Google Drive URL - need to convert to direct download link
    drive_url = "https://drive.google.com/file/d/1x4PO9OH0BHQFDAp-1rvuaEA-ZPI58CmV/view?usp=drive_link"
    file_id = "1x4PO9OH0BHQFDAp-1rvuaEA-ZPI58CmV"
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    gz_path = f"{db_path}.gz"
    
    logger.info("Database file not found. Downloading from Google Drive...")
    logger.info("Note: This is a large file (4.9GB) and may take some time to download.")
    
    try:
        # Download the gzipped file
        logger.info(f"Downloading to {gz_path}...")
        
        # Use requests with streaming for large files
        session = requests.Session()
        
        # First try the direct download
        response = session.get(download_url, stream=True)
        response.raise_for_status()
        
        # Check if we got the virus warning page by looking at content type and content
        content_type = response.headers.get('content-type', '')
        if 'text/html' in content_type or 'Google Drive - Virus scan warning' in response.text:
            logger.warning("Google Drive virus scan warning detected. Attempting to bypass...")
            # Parse the confirmation page to get the download link
            confirm_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            response = session.get(confirm_url, stream=True)
            response.raise_for_status()
            
            # If still getting HTML, we need to extract the actual download link
            if 'text/html' in response.headers.get('content-type', ''):
                import re
                # Look for the confirmation token in the HTML
                text_content = response.text
                confirm_match = re.search(r'confirm=([0-9A-Za-z_-]+)', text_content)
                if confirm_match:
                    confirm_token = confirm_match.group(1)
                    final_url = f"https://drive.google.com/uc?export=download&confirm={confirm_token}&id={file_id}"
                    logger.info("Using confirmation token to bypass virus warning...")
                    response = session.get(final_url, stream=True)
                    response.raise_for_status()
                else:
                    # Try alternative approach - look for download link in the page
                    download_link_match = re.search(r'href=["\'](/uc\?export=download[^"\']+)["\']', text_content)
                    if download_link_match:
                        download_path = download_link_match.group(1)
                        final_url = f"https://drive.google.com{download_path}"
                        logger.info("Found alternative download link...")
                        response = session.get(final_url, stream=True)
                        response.raise_for_status()
        
        # Final check - make sure we're not getting HTML
        if 'text/html' in response.headers.get('content-type', ''):
            logger.error("Still receiving HTML content. Download may have failed.")
            with open('debug_response.html', 'w') as f:
                f.write(response.text)
            logger.error("Response content saved to debug_response.html for inspection")
            return False
        
        # Save the gzipped file
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(gz_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        logger.info(f"Downloaded {downloaded/1024/1024:.1f}MB / {total_size/1024/1024:.1f}MB ({percent:.1f}%)")
        
        logger.info(f"Download completed: {gz_path}")
        
        # Extract the gzipped file
        logger.info(f"Extracting {gz_path} to {db_path}...")
        
        with gzip.open(gz_path, 'rb') as f_in:
            with open(db_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove the gzipped file
        os.remove(gz_path)
        
        logger.info(f"Database successfully extracted to: {db_path}")
        
        # Verify the file exists and has reasonable size
        if os.path.exists(db_path):
            file_size = os.path.getsize(db_path)
            logger.info(f"Database file size: {file_size/1024/1024:.1f}MB")
            return True
        else:
            logger.error("Database file extraction failed")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        # Clean up partial files
        if os.path.exists(gz_path):
            os.remove(gz_path)
        if os.path.exists(db_path):
            os.remove(db_path)
        return False

class IronCondorBacktester:
    def __init__(self, db_path: str = "option_data.duckdb", ticker: str = "SPXW", wing: int = 20, 
                 exclude_days: List[str] = None, exit_time: str = "13:00"):
        """
        Initialize the backtester.
        
        Args:
            db_path: Path to DuckDB database
            ticker: Ticker symbol (SPXW, SPY, QQQ, SPX)
            wing: Wing width for iron condor (1,2,3,4,5,10,15,20)
            exclude_days: List of days to exclude (Monday, Tuesday, Wednesday, Thursday, Friday)
            exit_time: Hard exit time (default: 13:00)
        """
        self.db_path = db_path
        self.ticker = ticker
        self.wing = wing
        self.exclude_days = exclude_days or []
        self.exit_time = exit_time
        self.profit_target = 0.10  # 10% profit target
        self.conn = None

    def connect(self):
        """Connect to DuckDB database."""
        try:
            # Ensure database file exists
            if not os.path.exists(self.db_path):
                logger.info(f"Database file {self.db_path} not found. Attempting to download...")
                if not download_database(self.db_path):
                    raise FileNotFoundError(f"Failed to download database file: {self.db_path}")
            
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            
    def get_trade_dates(self) -> List[str]:
        """Get all available trade dates for specified ticker."""
        # Map day names to strftime numbers
        day_map = {
            'Monday': '1',
            'Tuesday': '2', 
            'Wednesday': '3',
            'Thursday': '4',
            'Friday': '5',
            'Saturday': '6',
            'Sunday': '0'
        }
        
        # Build exclude condition
        exclude_conditions = []
        for day in self.exclude_days:
            if day in day_map:
                exclude_conditions.append(f"strftime('%w', trade_date) != '{day_map[day]}'")
        
        exclude_clause = " AND " + " AND ".join(exclude_conditions) if exclude_conditions else ""
        
        query = f"""
        SELECT DISTINCT trade_date 
        FROM optionData_Backtesting 
        WHERE ticker = '{self.ticker}' 
        {exclude_clause}
        ORDER BY trade_date
        """
        result = self.conn.execute(query).fetchall()
        return [row[0] for row in result]
        
    def get_entry_snapshot(self, trade_date: str, entry_time: str = '10:00') -> pd.DataFrame:
        """
        Get entry window data for a specific trade date and entry time.
        
        Args:
            trade_date: Trade date in YYYY-MM-DD format
            entry_time: Entry time (default: '10:00')
            
        Returns:
            DataFrame with entry window data
        """
        # Calculate the 5-minute window ending at entry_time
        entry_hour, entry_min = map(int, entry_time.split(':'))
        window_start_min = entry_min - 5
        window_start_hour = entry_hour
        
        if window_start_min < 0:
            window_start_min = 60 + window_start_min
            window_start_hour = entry_hour - 1
            
        start_time = f"{window_start_hour:02d}:{window_start_min:02d}:00"
        end_time = f"{entry_hour:02d}:{entry_min}:00"
        
        query = f"""
        SELECT 
            trade_date,
            data_timestamp,
            contract_right,
            contract_strike,
            data_bid,
            data_ask,
            data_delta,
            data_underlying_price,
            ticker
        FROM optionData_Backtesting 
        WHERE ticker = '{self.ticker}' 
        AND trade_date = ?
        AND strftime('%H:%M:%S', data_timestamp) BETWEEN ? AND ?
        AND data_bid > 0 AND data_ask > 0
        ORDER BY data_timestamp
        """
        return self.conn.execute(query, [trade_date, start_time, end_time]).df()
        
    def get_exit_data(self, trade_date: str, strikes: List[float], entry_timestamp: datetime) -> pd.DataFrame:
        """
        Get exit monitoring window data (after entry timestamp to 13:00:00) for specific strikes.
        """
        strike_str = ','.join([str(s) for s in strikes])
        entry_time_str = entry_timestamp.strftime('%H:%M:%S')
        query = f"""
        SELECT 
            trade_date,
            data_timestamp,
            contract_right,
            contract_strike,
            data_bid,
            data_ask,
            data_delta,
            data_underlying_price,
            ticker
        FROM optionData_Backtesting 
        WHERE ticker = '{self.ticker}' 
        AND trade_date = ?
        AND contract_strike IN ({strike_str})
        AND strftime('%H:%M:%S', data_timestamp) > ?
        AND strftime('%H:%M:%S', data_timestamp) <= '{self.exit_time}:00'
        AND data_bid > 0 AND data_ask > 0
        ORDER BY data_timestamp
        """
        return self.conn.execute(query, [trade_date, entry_time_str]).df()
        
    def calculate_mid_price(self, bid: float, ask: float) -> float:
        """Calculate mid price."""
        return (bid + ask) / 2.0
        
    def select_strikes(self, entry_data: pd.DataFrame, timestamp: datetime) -> Optional[Dict]:
        """
        Select strikes based on delta rules at a specific timestamp.
        
        Args:
            entry_data: DataFrame with entry window data
            timestamp: Specific timestamp to analyze
            
        Returns:
            Dictionary with selected strikes or None if selection fails
        """
        # Filter data for specific timestamp
        snapshot = entry_data[entry_data['data_timestamp'] == timestamp].copy()
        
        if snapshot.empty:
            return None
            
        # Separate calls and puts
        calls = snapshot[snapshot['contract_right'] == 'CALL'].copy()
        puts = snapshot[snapshot['contract_right'] == 'PUT'].copy()
        
        if calls.empty or puts.empty:
            return None
            
        # Calculate mid prices and deltas
        calls['mid_price'] = self.calculate_mid_price(calls['data_bid'], calls['data_ask'])
        puts['mid_price'] = self.calculate_mid_price(puts['data_bid'], puts['data_ask'])
        
        # Select SELL CALL: Highest delta < 0.20
        valid_calls = calls[calls['data_delta'] < 0.20]
        if valid_calls.empty:
            return None
        sell_call = valid_calls.loc[valid_calls['data_delta'].idxmax()]
        
        # Select SELL PUT: Lowest delta > -0.20
        valid_puts = puts[puts['data_delta'] > -0.20]
        if valid_puts.empty:
            return None
        sell_put = valid_puts.loc[valid_puts['data_delta'].idxmin()]
        
        # Calculate wing strikes
        buy_call_strike = sell_call['contract_strike'] + self.wing
        buy_put_strike = sell_put['contract_strike'] - self.wing
        
        # Find nearest available strikes for wings
        available_strikes = snapshot['contract_strike'].unique()
        
        # Buy Call: Nearest strike to calculated buy_call_strike
        buy_call_candidates = calls[calls['contract_strike'] >= buy_call_strike]
        if buy_call_candidates.empty:
            # If no higher strike, use the highest available
            buy_call = calls.loc[calls['contract_strike'].idxmax()]
        else:
            buy_call = buy_call_candidates.loc[buy_call_candidates['contract_strike'].idxmin()]
            
        # Buy Put: Nearest strike to calculated buy_put_strike
        buy_put_candidates = puts[puts['contract_strike'] <= buy_put_strike]
        if buy_put_candidates.empty:
            # If no lower strike, use the lowest available
            buy_put = puts.loc[puts['contract_strike'].idxmin()]
        else:
            buy_put = buy_put_candidates.loc[buy_put_candidates['contract_strike'].idxmax()]
        
        return {
            'sell_call_strike': sell_call['contract_strike'],
            'sell_put_strike': sell_put['contract_strike'],
            'buy_call_strike': buy_call['contract_strike'],
            'buy_put_strike': buy_put['contract_strike'],
            'sell_call_mid': sell_call['mid_price'],
            'sell_put_mid': sell_put['mid_price'],
            'buy_call_mid': buy_call['mid_price'],
            'buy_put_mid': buy_put['mid_price'],
            'sell_call_delta': sell_call['data_delta'],
            'sell_put_delta': sell_put['data_delta'],
            'underlying_price': sell_call['data_underlying_price'],
            'entry_timestamp': timestamp
        }
        
    def calculate_entry_credit(self, strikes: Dict) -> float:
        """Calculate iron condor entry credit."""
        return (strikes['sell_call_mid'] + strikes['sell_put_mid']) - \
               (strikes['buy_call_mid'] + strikes['buy_put_mid'])
               
    def calculate_exit_cost(self, exit_data: pd.DataFrame, strikes: Dict) -> Optional[float]:
        """Calculate exit cost at a specific timestamp (positive = cost, negative = credit)."""
        # Get data for all four strikes at this timestamp
        timestamp_data = exit_data[exit_data['data_timestamp'] == exit_data['data_timestamp'].iloc[0]]
        
        # Calculate mid prices for each leg
        mids = {}
        for _, row in timestamp_data.iterrows():
            strike = float(row['contract_strike'])  # Convert to float for comparison
            right = row['contract_right']
            mid = self.calculate_mid_price(row['data_bid'], row['data_ask'])
            
            # For closing: we need to buy what we sold, and sell what we bought
            if strike == strikes['sell_call_strike'] and right == 'CALL':
                # We sold this call at entry, now we need to BUY it to close
                mids['buy_close_call_mid'] = mid
            elif strike == strikes['sell_put_strike'] and right == 'PUT':
                # We sold this put at entry, now we need to BUY it to close
                mids['buy_close_put_mid'] = mid
            elif strike == strikes['buy_call_strike'] and right == 'CALL':
                # We bought this call at entry, now we need to SELL it to close
                mids['sell_close_call_mid'] = mid
            elif strike == strikes['buy_put_strike'] and right == 'PUT':
                # We bought this put at entry, now we need to SELL it to close
                mids['sell_close_put_mid'] = mid
                
        # Check if we have all legs
        if len(mids) != 4:
            return None
            
        # Calculate exit cost: (cost to close shorts) - (credit from closing longs)
        # We BUY back the shorts we sold, and SELL the longs we bought
        exit_cost = (mids['buy_close_call_mid'] + mids['buy_close_put_mid']) - \
                   (mids['sell_close_call_mid'] + mids['sell_close_put_mid'])
        
        return exit_cost
               
    def monitor_exit(self, trade_date: str, strikes: Dict, entry_credit: float) -> Dict:
        """
        Monitor exit conditions throughout the trading day.
        
        Returns:
            Dictionary with exit results
        """
        # Get exit data for the selected strikes
        strike_list = [
            strikes['sell_call_strike'],
            strikes['sell_put_strike'], 
            strikes['buy_call_strike'],
            strikes['buy_put_strike']
        ]
        
        exit_data = self.get_exit_data(trade_date, strike_list, strikes['entry_timestamp'])
        
        if exit_data.empty:
            # No exit data available
            return {
                'exit_reason': 'NO_DATA',
                'exit_timestamp': None,
                'exit_debit': None,
                'pnl': None,
                'pnl_pct': None
            }
            
        # Group by timestamp and check each timestamp
        unique_timestamps = exit_data['data_timestamp'].unique()
        
        for timestamp in sorted(unique_timestamps):
            timestamp_data = exit_data[exit_data['data_timestamp'] == timestamp]
            exit_cost = self.calculate_exit_cost(timestamp_data, strikes)
            
            if exit_cost is None:
                continue
                
            # Calculate P&L
            pnl = entry_credit - exit_cost
            pnl_pct = pnl / entry_credit if entry_credit > 0 else 0
            
            # Check profit target
            if pnl_pct >= self.profit_target:
                return {
                    'exit_reason': 'TP',
                    'exit_timestamp': timestamp,
                    'exit_cost': exit_cost,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct
                }
                
        # If profit target not hit, force exit at last timestamp with complete data
        unique_timestamps = exit_data['data_timestamp'].unique()
        
        # Find the last timestamp that has all 4 legs
        last_complete_timestamp = None
        final_cost = None
        
        for timestamp in sorted(unique_timestamps, reverse=True):
            timestamp_data = exit_data[exit_data['data_timestamp'] == timestamp]
            
            # Check if we have all 4 legs
            required_legs = 0
            for _, row in timestamp_data.iterrows():
                strike = float(row['contract_strike'])
                right = row['contract_right']
                
                if (strike == strikes['sell_call_strike'] and right == 'CALL') or \
                   (strike == strikes['sell_put_strike'] and right == 'PUT') or \
                   (strike == strikes['buy_call_strike'] and right == 'CALL') or \
                   (strike == strikes['buy_put_strike'] and right == 'PUT'):
                    required_legs += 1
            
            if required_legs == 4:
                last_complete_timestamp = timestamp
                final_cost = self.calculate_exit_cost(timestamp_data, strikes)
                break
        
        if final_cost is not None:
            pnl = entry_credit - final_cost
            pnl_pct = pnl / entry_credit if entry_credit > 0 else 0
        else:
            pnl = None
            pnl_pct = None
            last_complete_timestamp = None
            
        return {
            'exit_reason': 'HARD',
            'exit_timestamp': last_complete_timestamp,
            'exit_cost': final_cost,
            'pnl': pnl,
            'pnl_pct': pnl_pct
        }
        
    def process_trade_date(self, trade_date: str, entry_time: str = '10:00') -> Optional[Dict]:
        """
        Process a single trade date with specified entry time.
        
        Args:
            trade_date: Trade date in YYYY-MM-DD format
            entry_time: Entry time (default: '10:00')
        
        Returns:
            Dictionary with trade results or None if no valid trade
        """
        # Get entry window data
        entry_data = self.get_entry_snapshot(trade_date, entry_time)
        
        if entry_data.empty:
            logger.warning(f"No entry data for {trade_date} at {entry_time}")
            return None
            
        # Use the latest timestamp in entry window (closest to entry_time)
        entry_timestamp = entry_data['data_timestamp'].max()
        
        # Select strikes
        strikes = self.select_strikes(entry_data, entry_timestamp)
        if not strikes:
            logger.warning(f"No valid strikes for {trade_date} at {entry_time}")
            return None
            
        # Calculate entry credit
        entry_credit = self.calculate_entry_credit(strikes)
        
        if entry_credit <= 0:
            logger.warning(f"Non-positive entry credit for {trade_date}: {entry_credit}")
            return None
            
        # Monitor exit
        exit_results = self.monitor_exit(trade_date, strikes, entry_credit)
        
        # Combine results
        # Convert trade_date to datetime to get day of week
        trade_datetime = pd.to_datetime(trade_date)
        day_of_week = trade_datetime.strftime('%A')
        
        trade_result = {
            'trade_date': trade_date,
            'day_of_week': day_of_week,
            'ticker': 'SPXW',
            'wing': self.wing,
            'entry_timestamp': entry_timestamp,
            'underlying_price_entry': strikes['underlying_price'],
            'sell_call_strike': strikes['sell_call_strike'],
            'sell_put_strike': strikes['sell_put_strike'],
            'buy_call_strike': strikes['buy_call_strike'],
            'buy_put_strike': strikes['buy_put_strike'],
            'sell_call_delta': strikes['sell_call_delta'],
            'sell_put_delta': strikes['sell_put_delta'],
            'entry_credit': entry_credit,
            **exit_results
        }
        
        return trade_result
        
    def run_backtest(self, start_date: str = None, end_date: str = None, entry_time: str = '10:00') -> pd.DataFrame:
        """
        Run the complete backtest.
        
        Args:
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            entry_time: Entry time (default: '10:00')
            
        Returns:
            DataFrame with all trade results
        """
        self.connect()
        
        try:
            # Get trade dates
            all_dates = self.get_trade_dates()
            
            # Filter by date range if specified
            if start_date:
                all_dates = [d for d in all_dates if str(d) >= start_date]
            if end_date:
                all_dates = [d for d in all_dates if str(d) <= end_date]
                
            logger.info(f"Processing {len(all_dates)} trade dates with entry time {entry_time}")
            
            # Process each trade date
            results = []
            for i, trade_date in enumerate(all_dates):
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(all_dates)} dates")
                    
                result = self.process_trade_date(trade_date, entry_time)
                if result:
                    results.append(result)
                    
            # Create results DataFrame
            if results:
                results_df = pd.DataFrame(results)
                
                # Calculate summary statistics
                total_trades = len(results_df)
                profitable_trades = (results_df['pnl'] > 0).sum()
                win_rate = profitable_trades / total_trades * 100
                
                avg_pnl = results_df['pnl'].mean()
                avg_pnl_pct = results_df['pnl_pct'].mean() * 100
                
                total_pnl = results_df['pnl'].sum()
                
                # Add max and min P&L
                max_pnl = results_df['pnl'].max()
                min_pnl = results_df['pnl'].min()
                max_pnl_pct = results_df['pnl_pct'].max() * 100
                min_pnl_pct = results_df['pnl_pct'].min() * 100
                
                logger.info(f"\n=== BACKTEST RESULTS ===")
                logger.info(f"Total Trades: {total_trades}")
                logger.info(f"Win Rate: {win_rate:.1f}%")
                logger.info(f"Average P&L: ${avg_pnl:.2f}")
                logger.info(f"Average P&L %: {avg_pnl_pct:.2f}%")
                logger.info(f"Max P&L: ${max_pnl:.2f}")
                logger.info(f"Min P&L: ${min_pnl:.2f}")
                logger.info(f"Max P&L %: {max_pnl_pct:.2f}%")
                logger.info(f"Min P&L %: {min_pnl_pct:.2f}%")
                logger.info(f"Total P&L: ${total_pnl:.2f}")
                
                return results_df
            else:
                logger.warning("No valid trades found")
                return pd.DataFrame()
                
        finally:
            self.close()

    def test_multiple_entry_times(self, start_date: str = None, end_date: str = None, 
                                   entry_times: List[str] = ['09:55', '09:56', '09:57', '09:58', '09:59', '10:00']) -> pd.DataFrame:
        """
        Test multiple entry times and compare results.
        
        Args:
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            entry_times: List of entry times to test
            
        Returns:
            DataFrame with comparison results for all entry times
        """
        all_results = {}
        
        for entry_time in entry_times:
            logger.info(f"\n=== Testing entry time: {entry_time} ===")
            results = self.run_backtest(start_date, end_date, entry_time)
            
            if not results.empty:
                # Calculate summary statistics
                total_trades = len(results)
                profitable_trades = (results['pnl'] > 0).sum()
                win_rate = profitable_trades / total_trades * 100
                avg_pnl = results['pnl'].mean()
                avg_pnl_pct = results['pnl_pct'].mean() * 100
                total_pnl = results['pnl'].sum()
                
                # Add max and min P&L for summary only
                max_pnl = results['pnl'].max()
                min_pnl = results['pnl'].min()
                
                # Store summary
                all_results[entry_time] = {
                    'total_trades': total_trades,
                    'win_rate': win_rate,
                    'avg_pnl': avg_pnl,
                    'avg_pnl_pct': avg_pnl_pct,
                    'max_pnl': max_pnl,
                    'min_pnl': min_pnl,
                    'total_pnl': total_pnl
                }
                
                # Save individual results
                filename = f'backtest_results_{entry_time.replace(":", "")}.csv'
                results.to_csv(filename, index=False)
                logger.info(f"Results for {entry_time} saved to {filename}")
                
                logger.info(f"Results for {entry_time}:")
                logger.info(f"  Total Trades: {total_trades}")
                logger.info(f"  Win Rate: {win_rate:.1f}%")
                logger.info(f"  Average P&L: ${avg_pnl:.2f}")
                logger.info(f"  Average P&L %: {avg_pnl_pct:.2f}%")
                logger.info(f"  Max P&L: ${max_pnl:.2f}")
                logger.info(f"  Min P&L: ${min_pnl:.2f}")
                logger.info(f"  Total P&L: ${total_pnl:.2f}")
            else:
                logger.warning(f"No valid trades for entry time {entry_time}")
        
        # Create comparison DataFrame
        if all_results:
            comparison_df = pd.DataFrame.from_dict(all_results, orient='index')
            comparison_df = comparison_df.round(2)
            logger.info(f"\n=== ENTRY TIME COMPARISON ===")
            logger.info(comparison_df.to_string())
            
            return comparison_df
        else:
            logger.warning("No results for any entry time")
            return pd.DataFrame()
        
def main():
    """Main function to run the backtest."""
    import argparse
    
    parser = argparse.ArgumentParser(description='0DTE Iron Condor Backtest')
    parser.add_argument('--ticker', type=str, default='SPXW', choices=['SPXW', 'SPY', 'QQQ', 'SPX'],
                       help='Ticker symbol (default: SPXW)')
    parser.add_argument('--wing', type=int, default=20, choices=[1, 2, 3, 4, 5, 10, 15, 20],
                       help='Wing width for iron condor (default: 20)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='backtest_results.csv',
                       help='Output CSV file (default: backtest_results.csv)')
    parser.add_argument('--entry-time', type=str, nargs='+',
                       help='Entry time(s) in HH:MM format (default: 10:00, can pass multiple)')
    parser.add_argument('--test-multiple-times', action='store_true',
                       help='Test multiple entry times (09:55 to 10:00)')
    parser.add_argument('--exclude-days', type=str, nargs='+', 
                       choices=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
                       help='Days to exclude from trading (default: None)')
    parser.add_argument('--exit-time', type=str, default='13:00',
                       help='Hard exit time in HH:MM format (default: 13:00)')
    parser.add_argument('--download-db', action='store_true',
                       help='Download database file from Google Drive if not present')
    parser.add_argument('--force-download', action='store_true',
                       help='Force download database even if it already exists')
    
    args = parser.parse_args()
    
    # Handle database download if requested
    if args.download_db or args.force_download:
        db_path = "option_data.duckdb"
        logger.info("Database download requested...")
        if download_database(db_path, force_download=args.force_download):
            logger.info("Database download completed successfully")
        else:
            logger.error("Database download failed")
            return
    
    # Handle entry times
    if args.test_multiple_times:
        entry_times = ['09:55', '09:56', '09:57', '09:58', '09:59', '10:00']
    elif args.entry_time:
        entry_times = args.entry_time
    else:
        entry_times = ['10:00']
    
    # Run backtest
    backtester = IronCondorBacktester(
        ticker=args.ticker,
        wing=args.wing,
        exclude_days=args.exclude_days,
        exit_time=args.exit_time
    )
    
    if len(entry_times) == 1:
        # Run single backtest
        results = backtester.run_backtest(args.start_date, args.end_date, entry_times[0])
        
        if not results.empty:
            # Save results
            results.to_csv(args.output, index=False)
            logger.info(f"Results saved to {args.output}")
            
            # Display sample results
            print("\nSample Results:")
            print(results[['trade_date', 'sell_call_strike', 'sell_put_strike', 
                          'entry_credit', 'pnl', 'pnl_pct', 'exit_reason']].head(10))
    else:
        # Test multiple entry times
        results = backtester.test_multiple_entry_times(args.start_date, args.end_date, entry_times)
        
        if not results.empty:
            # Save comparison results
            results.to_csv('entry_time_comparison.csv')
            logger.info("Comparison results saved to entry_time_comparison.csv")

if __name__ == "__main__":
    main()
