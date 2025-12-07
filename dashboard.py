import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

time_options = []
for hour in range(9, 17):  # 9 to 16
    for minute in range(0, 60):
        if hour == 9 and minute < 30:  # Skip before 9:30
            continue
        if hour == 16 and minute > 0:  # Skip after 16:00
            continue
        time_options.append(f"{hour:02d}:{minute:02d}")

# Add the current directory to the path to import the backtester
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtesting_0dte_SPXW import IronCondorBacktester, download_database

def display_single_results(results, ticker, wing, entry_time, exclude_days, exit_time):
    """Display results for single entry time backtest."""
    
    # Calculate P&L per contract (multiply by 100)
    results['pnl'] = results['pnl'] * 100
    results['pnl_pct'] = results['pnl_pct'] * 100  # Convert to percentage
    
    # Summary statistics
    st.subheader("📊 Summary Statistics")
    
    total_trades = len(results)
    profitable_trades = (results['pnl'] > 0).sum()
    win_rate = profitable_trades / total_trades * 100
    avg_pnl = results['pnl'].mean()
    total_pnl = results['pnl'].sum()
    max_pnl = results['pnl'].max()
    min_pnl = results['pnl'].min()
    
    # Calculate Annual ROI = (total_pnl / wing) / 3 * 100 (as percentage)
    annual_roi = (total_pnl / wing / 3) if wing > 0 else 0
    
    # Display metrics in columns
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Trades", total_trades)
    with col2:
        st.metric("Win Rate", f"{win_rate:.1f}%")
    with col3:
        st.metric("Avg P&L", f"${avg_pnl:,.2f}")
    with col4:
        st.metric("Total P&L", f"${total_pnl:,.2f}")
    with col5:
        st.metric("Annual ROI", f"{annual_roi:,.2f}%")
    
    # Parameters summary
    st.subheader("⚙️ Parameters Used")
    params_text = f"""
    **Ticker:** {ticker} | **Wing:** {wing} | **Entry Time:** {entry_time} | **Exit Time:** {exit_time}
    **Exclude Days:** {', '.join(exclude_days) if exclude_days else 'None'}
    **Date Range:** {results['trade_date'].min()} to {results['trade_date'].max()}
    **Total P&L:** ${total_pnl:,.2f} | **Annual ROI:** {annual_roi:,.2f}%
    """
    st.markdown(params_text)
    
    # P&L by Day of Week
    st.subheader("📅 P&L by Day of Week")
    
    day_pnl = results.groupby('day_of_week')['pnl'].agg(['sum', 'count', 'mean']).reset_index()
    day_pnl.columns = ['Day', 'Total P&L', 'Trade Count', 'Avg P&L']
    day_pnl = day_pnl.sort_values('Day')
    
    # Bar chart
    fig = px.bar(
        day_pnl,
        x='Day',
        y='Total P&L',
        title='Total P&L by Day of Week',
        color='Total P&L',
        color_continuous_scale=['red', 'yellow', 'green'],
        hover_data=['Trade Count', 'Avg P&L']
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, width='stretch')
    
    # Detailed results table
    st.subheader("📋 Detailed Results")
    
    # Format the data for display
    display_results = results.copy()
    display_results['pnl'] = display_results['pnl'].round(2)
    display_results['pnl_pct'] = display_results['pnl_pct'].round(2)
    display_results['entry_credit'] = display_results['entry_credit'].round(2)
    
    st.dataframe(
        display_results[['trade_date', 'day_of_week', 'entry_credit', 'pnl', 'pnl_pct', 'exit_reason']],
        width='stretch',
        hide_index=True
    )
    
    # Download results
    csv = display_results.to_csv(index=False)
    st.download_button(
        label="📥 Download Results CSV",
        data=csv,
        file_name=f"backtest_results_{ticker}_{entry_time.replace(':', '')}.csv",
        mime="text/csv"
    )

def display_multiple_results(results, ticker, wing, entry_times, exclude_days, exit_time):
    """Display results for multiple entry times backtest."""
    
    # Calculate P&L per contract (multiply by 100) and convert to percentage
    results['total_pnl'] = results['total_pnl'] * 100
    results['avg_pnl'] = results['avg_pnl'] * 100
    results['win_rate'] = results['win_rate'] * 100  # Already a percentage, just to be safe
    
    st.subheader("📊 Entry Time Comparison")
    
    # Add Annual ROI column
    results['annual_roi'] = (results['total_pnl'] / wing / 3).round(2)
    
    # Format the display
    display_df = results.copy()
    display_df['total_pnl'] = display_df['total_pnl'].apply(lambda x: f"${x:,.2f}")
    display_df['avg_pnl'] = display_df['avg_pnl'].apply(lambda x: f"${x:,.2f}")
    display_df['win_rate'] = display_df['win_rate'].apply(lambda x: f"{x:.1f}%")
    display_df['annual_roi'] = display_df['annual_roi'].apply(lambda x: f"{x:.2f}%")
    
    # Display comparison table
    st.dataframe(display_df, width='stretch')
    
    # Create bar chart comparing total P&L by entry time
    st.subheader("📈 Total P&L by Entry Time")
    
    fig = px.bar(
        x=results.index,
        y=results['total_pnl'],
        title=f'Total P&L by Entry Time - {ticker}',
        color=results['total_pnl'],
        color_continuous_scale=['red', 'yellow', 'green'],
        labels={'x': 'Entry Time', 'y': 'Total P&L ($)'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, width='stretch')
    
    # Annual ROI comparison
    st.subheader("📊 Annual ROI by Entry Time")
    
    fig3 = px.bar(
        x=results.index,
        y=results['annual_roi'],
        title=f'Annual ROI by Entry Time - {ticker}',
        color=results['annual_roi'],
        color_continuous_scale='viridis',
        labels={'x': 'Entry Time', 'y': 'Annual ROI (%)'}
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, width='stretch')
    
    # Parameters summary
    st.subheader("⚙️ Parameters Used")
    params_text = f"""
    **Ticker:** {ticker} | **Wing:** {wing} | **Entry Times:** {', '.join(entry_times)} | **Exit Time:** {exit_time}
    **Exclude Days:** {', '.join(exclude_days) if exclude_days else 'None'}
    """
    st.markdown(params_text)
    
    # Download comparison results
    csv = results.to_csv()
    st.download_button(
        label="📥 Download Comparison CSV",
        data=csv,
        file_name=f"entry_time_comparison_{ticker}.csv",
        mime="text/csv"
    )

# Page configuration
st.set_page_config(
    page_title="0DTE Iron Condor Backtesting Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("📊 0DTE Iron Condor Backtesting Dashboard")
st.markdown("---")

# Sidebar for filters
st.sidebar.header("🔧 Backtesting Parameters")

# Ticker selection
ticker = st.sidebar.selectbox(
    "Ticker",
    options=["SPXW", "SPY", "QQQ", "SPX"],
    index=0,
    help="Select the underlying ticker to backtest"
)

# Wing size selection
wing = st.sidebar.selectbox(
    "Wing Size",
    options=[1, 2, 3, 4, 5, 10, 15, 20],
    index=7,
    help="Width of the iron condor wings"
)

# Entry time selection
entry_time_option = st.sidebar.radio(
    "Entry Time Selection",
    options=["Single Time", "Multiple Times", "Preset Range"],
    help="Choose how to select entry times"
)

if entry_time_option == "Single Time":
    entry_time_str = st.sidebar.selectbox(
        "Entry Time (EST)",
        options=time_options,
        index=time_options.index("10:00"),
        help="Select entry time (9:30-16:00 EST)"
    )
    entry_times = [entry_time_str]
    
elif entry_time_option == "Multiple Times":
    entry_times_input = st.sidebar.text_input(
        "Entry Times (comma-separated)",
        value="09:55, 10:00",
        help="Enter multiple times in HH:MM format, separated by commas"
    )
    entry_times = [t.strip() for t in entry_times_input.split(",")]
else:  # Preset Range
    entry_times = ["09:55", "09:56", "09:57", "09:58", "09:59", "10:00"]

# Exclude days selection
exclude_days = st.sidebar.multiselect(
    "Exclude Days",
    options=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
    default=[],
    help="Select days to exclude from trading"
)

# Exit time selection
exit_time_str = st.sidebar.selectbox(
    "Exit Time (EST)",
    options=time_options,
    index=time_options.index("13:00"),
    help="Select hard exit time (9:30-16:00 EST)"
)
exit_time = pd.to_datetime(exit_time_str).time()

# Date range selection
st.sidebar.subheader("📅 Date Range")
start_date = st.sidebar.date_input(
    "Start Date",
    value=pd.to_datetime("2021-01-01"),
    help="Start date for backtest"
)
end_date = st.sidebar.date_input(
    "End Date", 
    value=pd.to_datetime("2025-12-31"),
    help="End date for backtest"
)

# Run backtest button
run_button = st.sidebar.button(
    "🚀 Run Backtest",
    type="primary",
    help="Click to run the backtest with selected parameters"
)

# Main content area
if run_button:
    st.header("📈 Backtest Results")
    
    # Show progress
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Check and download database if needed
        status_text.text("Checking database...")
        progress_bar.progress(5)
        
        db_path = "option_data.duckdb"
        if not os.path.exists(db_path):
            status_text.text("Downloading database from Google Drive...")
            if not download_database(db_path):
                st.error("Failed to download database. Please try again.")
                st.stop()
            status_text.text("Database downloaded successfully!")
        
        # Initialize backtester
        status_text.text("Initializing backtester...")
        progress_bar.progress(10)
        
        backtester = IronCondorBacktester(
            ticker=ticker,
            wing=wing,
            exclude_days=exclude_days,
            exit_time=exit_time_str
        )
        
        # Run backtest
        status_text.text("Running backtest...")
        progress_bar.progress(30)
        
        if len(entry_times) == 1:
            # Single entry time
            results = backtester.run_backtest(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                entry_time=entry_times[0]
            )
        else:
            # Multiple entry times
            results = backtester.test_multiple_entry_times(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                entry_times=entry_times
            )
        
        progress_bar.progress(80)
        status_text.text("Processing results...")
        
        if results.empty:
            st.error("No results found for the selected parameters.")
        else:
            progress_bar.progress(100)
            status_text.text("✅ Backtest completed!")
            
            # Display results
            if len(entry_times) == 1:
                # Single entry time results
                display_single_results(results, ticker, wing, entry_times[0], exclude_days, exit_time.strftime("%H:%M"))
            else:
                # Multiple entry times results
                display_multiple_results(results, ticker, wing, entry_times, exclude_days, exit_time.strftime("%H:%M"))
                
    except Exception as e:
        st.error(f"Error running backtest: {str(e)}")
        progress_bar.progress(0)
        status_text.text("")

# Instructions section
st.sidebar.markdown("---")
st.sidebar.markdown("### 📖 Instructions")
st.sidebar.markdown("""
1. **Select Parameters**: Choose your backtesting parameters in the sidebar
2. **Run Backtest**: Click the "Run Backtest" button
3. **View Results**: Results will appear below with charts and statistics
4. **Download Data**: Export results as CSV for further analysis

**Note**: First run may take a few minutes as it processes the data.
""")

# Footer
st.markdown("---")
st.markdown("💡 **Tip**: Use the exclude days feature to avoid historically poor-performing days like Tuesdays.")