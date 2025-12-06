import pandas as pd
import numpy as np

def analyze_all_entry_times_combined():
    """Analyze losses by day of week across ALL entry times combined."""
    
    entry_times = ['09:55', '09:56', '09:57', '09:58', '09:59', '10:00']
    
    print("=== COMBINED ANALYSIS - ALL ENTRY TIMES (6,060 total trades) ===\n")
    
    # Combine all data
    all_data = []
    
    for entry_time in entry_times:
        filename = f'backtest_results_{entry_time.replace(":", "")}.csv'
        
        try:
            df = pd.read_csv(filename)
            df['entry_time'] = entry_time
            all_data.append(df)
        except FileNotFoundError:
            print(f"Warning: {filename} not found")
            continue
    
    if not all_data:
        print("No data found")
        return
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    print(f"Total Trades Analyzed: {len(combined_df)}")
    print(f"Date Range: {combined_df['trade_date'].min()} to {combined_df['trade_date'].max()}")
    print(f"Entry Times: {', '.join(entry_times)}\n")
    
    # Overall loss analysis
    losing_trades = combined_df[combined_df['pnl'] < 0]
    total_trades = len(combined_df)
    total_losing = len(losing_trades)
    overall_loss_rate = (total_losing / total_trades * 100)
    
    print(f"Overall Loss Rate: {overall_loss_rate:.1f}% ({total_losing} losing trades out of {total_trades})")
    
    # Loss analysis by day of week
    day_analysis = losing_trades.groupby('day_of_week').agg({
        'pnl': ['count', 'sum', 'mean'],
        'pnl_pct': ['mean']
    }).round(2)
    
    day_analysis.columns = ['Num_Losses', 'Total_Loss', 'Avg_Loss', 'Avg_Loss_Pct']
    
    # Calculate total trades and loss rate by day
    total_by_day = combined_df.groupby('day_of_week').size()
    loss_rate_by_day = (day_analysis['Num_Losses'] / total_by_day * 100).round(1)
    
    print(f"\n=== LOSSES BY DAY OF WEEK (ALL ENTRY TIMES COMBINED) ===")
    
    # Sort by total loss (worst first)
    sorted_days = day_analysis.sort_values('Total_Loss').index
    
    for day in sorted_days:
        if day in day_analysis.index:
            losses = day_analysis.loc[day]
            total_trades_day = total_by_day[day]
            rate = loss_rate_by_day[day]
            
            print(f"{day}:")
            print(f"  Losses: {losses['Num_Losses']}/{total_trades_day} trades ({rate}%)")
            print(f"  Average Loss: ${losses['Avg_Loss']:.2f}")
            print(f"  Total Loss: ${losses['Total_Loss']:.2f}")
            print()
    
    # Best and worst days
    worst_day = day_analysis['Total_Loss'].idxmin()
    best_day = day_analysis['Total_Loss'].idxmax()
    
    print(f"=== SUMMARY ===")
    print(f"Worst Day: {worst_day} (${day_analysis.loc[worst_day, 'Total_Loss']:.2f} total loss)")
    print(f"Best Day: {best_day} (${day_analysis.loc[best_day, 'Total_Loss']:.2f} total loss)")
    
    # Entry time analysis by day
    print(f"\n=== ENTRY TIME PERFORMANCE BY DAY ===")
    
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
        day_data = combined_df[combined_df['day_of_week'] == day]
        day_losing = day_data[day_data['pnl'] < 0]
        
        if len(day_data) > 0:
            loss_rate = len(day_losing) / len(day_data) * 100
            avg_loss = day_losing['pnl'].mean() if len(day_losing) > 0 else 0
            total_loss = day_losing['pnl'].sum() if len(day_losing) > 0 else 0
            
            print(f"\n{day} (All Entry Times):")
            print(f"  Loss Rate: {loss_rate:.1f}%")
            print(f"  Average Loss: ${avg_loss:.2f}")
            print(f"  Total Loss: ${total_loss:.2f}")
            
            # Best/worst entry time for this day
            entry_performance = day_data.groupby('entry_time')['pnl'].sum().sort_values()
            worst_entry = entry_performance.index[0]
            best_entry = entry_performance.index[-1]
            
            print(f"    Worst Entry Time: {worst_entry} (${entry_performance[worst_entry]:.2f})")
            print(f"    Best Entry Time: {best_entry} (${entry_performance[best_entry]:.2f})")

if __name__ == "__main__":
    analyze_all_entry_times_combined()
