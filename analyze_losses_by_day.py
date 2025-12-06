import pandas as pd
import numpy as np

def analyze_losses_by_day():
    """Analyze losses by day of week for each entry time."""
    
    entry_times = ['09:55', '09:56', '09:57', '09:58', '09:59', '10:00']
    
    print("=== LOSSES BY DAY OF WEEK ANALYSIS ===\n")
    
    for entry_time in entry_times:
        filename = f'backtest_results_{entry_time.replace(":", "")}.csv'
        
        try:
            df = pd.read_csv(filename)
            
            # Filter for losing trades only
            losing_trades = df[df['pnl'] < 0].copy()
            
            if losing_trades.empty:
                print(f"{entry_time}: No losing trades")
                continue
            
            # Group by day of week
            day_analysis = losing_trades.groupby('day_of_week').agg({
                'pnl': ['count', 'sum', 'mean'],
                'pnl_pct': ['mean']
            }).round(2)
            
            # Flatten column names
            day_analysis.columns = ['Num_Losses', 'Total_Loss', 'Avg_Loss', 'Avg_Loss_Pct']
            
            # Calculate total trades per day for context
            total_by_day = df.groupby('day_of_week').size()
            loss_rate = (day_analysis['Num_Losses'] / total_by_day * 100).round(1)
            
            print(f"=== {entry_time} Entry Time ===")
            print(f"Total Losing Trades: {len(losing_trades)} out of {len(df)} ({len(losing_trades)/len(df)*100:.1f}%)")
            print("\nLosses by Day:")
            
            for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                if day in day_analysis.index:
                    losses = day_analysis.loc[day]
                    total_trades_day = total_by_day[day]
                    rate = loss_rate[day]
                    
                    print(f"  {day}: {losses['Num_Losses']} losses ({rate}%) | "
                          f"Avg Loss: ${losses['Avg_Loss']:.2f} | "
                          f"Total Loss: ${losses['Total_Loss']:.2f}")
                else:
                    print(f"  {day}: No data")
            
            # Find worst day
            if not day_analysis.empty:
                worst_day = day_analysis['Total_Loss'].idxmin()
                worst_loss = day_analysis.loc[worst_day, 'Total_Loss']
                print(f"\n  Worst Day: {worst_day} (${worst_loss:.2f} total loss)")
            
            print()
            
        except FileNotFoundError:
            print(f"{entry_time}: File not found")
            print()

if __name__ == "__main__":
    analyze_losses_by_day()
