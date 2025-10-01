"""
MASTEK Hourly Distribution Analysis

This script analyzes the MASTEK hourly stock data to create 5 normal distributions
for each weekday (Monday-Friday), showing the frequency of hourly percentage changes
on the X-axis and frequency on the Y-axis.

Based on weekdaysDistribution.py but adapted for hourly data.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from scipy import stats
from scipy.stats import norm
import warnings
warnings.filterwarnings('ignore')

# Configure histogram limits and resolution (percentage units)
PCT_MIN = -20.0
PCT_MAX = 20.0
BIN_WIDTH = 0.1
BINS = np.arange(PCT_MIN, PCT_MAX + BIN_WIDTH, BIN_WIDTH)

def load_and_process_hourly_data(csv_path):
    """Load CSV data and process timestamps and calculate hourly percentage changes."""
    print("Loading hourly data...")
    df = pd.read_csv(csv_path)
    
    # Convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort by timestamp to ensure proper ordering
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Calculate hourly percentage change
    df['pct_change'] = ((df['close'] - df['open']) / df['open'] * 100)
    
    # Extract weekday from timestamp (0=Monday, 1=Tuesday, ..., 4=Friday, 5=Saturday, 6=Sunday)
    df['weekday'] = df['timestamp'].dt.dayofweek
    
    # Filter for weekdays only (0-4, Monday to Friday)
    df = df[df['weekday'] <= 4].copy()
    
    # Remove rows with NaN percentage changes
    df = df.dropna(subset=['pct_change'])

    # Keep percentage changes within requested range
    df = df[(df['pct_change'] >= PCT_MIN) & (df['pct_change'] <= PCT_MAX)].copy()
    
    # Create weekday names
    weekday_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday'}
    df['weekday_name'] = df['weekday'].map(weekday_names)
    
    # Add hour information for additional analysis
    df['hour'] = df['timestamp'].dt.hour
    
    print(f"Loaded {len(df)} hourly data points")
    print(f"Date range: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")
    print(f"Time range: {df['timestamp'].dt.time.min()} to {df['timestamp'].dt.time.max()}")
    
    return df

def calculate_statistics_by_weekday(df):
    """Calculate basic statistics for each weekday."""
    print("\n" + "="*60)
    print("HOURLY WEEKDAY STATISTICS SUMMARY")
    print("="*60)
    
    stats_summary = []
    
    for weekday in range(5):
        weekday_data = df[df['weekday'] == weekday]['pct_change']
        weekday_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'][weekday]
        
        stats_dict = {
            'Weekday': weekday_name,
            'Count': len(weekday_data),
            'Mean (%)': weekday_data.mean(),
            'Std Dev (%)': weekday_data.std(),
            'Min (%)': weekday_data.min(),
            'Max (%)': weekday_data.max(),
            'Median (%)': weekday_data.median(),
            'Skewness': stats.skew(weekday_data),
            'Kurtosis': stats.kurtosis(weekday_data)
        }
        stats_summary.append(stats_dict)
        
        print(f"\n{weekday_name}:")
        print(f"  Count: {stats_dict['Count']} hours")
        print(f"  Mean: {stats_dict['Mean (%)']:.4f}%")
        print(f"  Std Dev: {stats_dict['Std Dev (%)']:.4f}%")
        print(f"  Range: {stats_dict['Min (%)']:.2f}% to {stats_dict['Max (%)']:.2f}%")
        print(f"  Skewness: {stats_dict['Skewness']:.4f}")
        print(f"  Kurtosis: {stats_dict['Kurtosis']:.4f}")
    
    return pd.DataFrame(stats_summary)

def calculate_detailed_hourly_statistics(df):
    """Calculate detailed statistics for each hour of each weekday (30 combinations)."""
    print("\n" + "="*80)
    print("DETAILED 30 HOUR-WEEKDAY COMBINATION STATISTICS")
    print("="*80)
    
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    trading_hours = [3, 4, 5, 6, 7, 8, 9]
    
    detailed_stats = []
    
    for weekday in range(5):
        weekday_name = weekday_names[weekday]
        print(f"\n{weekday_name.upper()}:")
        print("-" * 60)
        
        for hour in trading_hours:
            # Get data for this specific hour and weekday
            hour_data = df[
                (df['weekday'] == weekday) & 
                (df['hour'] == hour)
            ]['pct_change']
            
            # Filter within configured range
            hour_data = hour_data[
                (hour_data >= PCT_MIN) & 
                (hour_data <= PCT_MAX)
            ]
            
            hour_label = f"{hour:02d}:45"
            
            if len(hour_data) > 0:
                stats_dict = {
                    'Weekday': weekday_name,
                    'Hour': hour_label,
                    'Count': len(hour_data),
                    'Mean (%)': hour_data.mean(),
                    'Std Dev (%)': hour_data.std(),
                    'Min (%)': hour_data.min(),
                    'Max (%)': hour_data.max(),
                    'Median (%)': hour_data.median(),
                    'Skewness': stats.skew(hour_data) if len(hour_data) > 1 else np.nan,
                    'Kurtosis': stats.kurtosis(hour_data) if len(hour_data) > 1 else np.nan
                }
                
                print(f"  {hour_label}: n={len(hour_data):3d}, μ={hour_data.mean():6.3f}%, σ={hour_data.std():6.3f}%, range=[{hour_data.min():6.2f}%, {hour_data.max():6.2f}%]")
            else:
                stats_dict = {
                    'Weekday': weekday_name,
                    'Hour': hour_label,
                    'Count': 0,
                    'Mean (%)': np.nan,
                    'Std Dev (%)': np.nan,
                    'Min (%)': np.nan,
                    'Max (%)': np.nan,
                    'Median (%)': np.nan,
                    'Skewness': np.nan,
                    'Kurtosis': np.nan
                }
                print(f"  {hour_label}: No data")
            
            detailed_stats.append(stats_dict)
    
    return pd.DataFrame(detailed_stats)

def create_hourly_weekday_distributions(df):
    """Create 30 individual distribution plots - one for each hour of each weekday."""
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    trading_hours = [3, 4, 5, 6, 7, 8, 9]  # Trading hours: 03:45, 04:45, ..., 09:45
    
    # Create a large figure with 6 rows (hours) and 5 columns (weekdays)
    fig, axes = plt.subplots(7, 5, figsize=(25, 35))
    fig.suptitle('MASTEK Stock: Individual Hour-Weekday Distribution Analysis\n(30 Individual Distributions)', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    print("\nGenerating 30 individual hour-weekday distributions...")
    
    # Create distribution for each hour of each weekday
    for hour_idx, hour in enumerate(trading_hours):
        for weekday_idx, weekday in enumerate(range(5)):
            ax = axes[hour_idx, weekday_idx]
            
            # Get data for this specific hour and weekday
            hour_weekday_data = df[
                (df['weekday'] == weekday) & 
                (df['hour'] == hour)
            ]['pct_change']
            
            # Filter within configured range
            hour_weekday_data = hour_weekday_data[
                (hour_weekday_data >= PCT_MIN) & 
                (hour_weekday_data <= PCT_MAX)
            ]
            
            weekday_name = weekday_names[weekday]
            hour_label = f"{hour:02d}:45"
            
            if len(hour_weekday_data) > 0:
                # Create histogram
                n, bins, patches = ax.hist(
                    hour_weekday_data,
                    bins=BINS,
                    density=False,
                    alpha=0.7,
                    color=colors[weekday_idx],
                    edgecolor='black',
                    linewidth=0.3,
                )
                
                # Fit normal distribution if we have enough data
                if len(hour_weekday_data) >= 3:
                    mu, sigma = norm.fit(hour_weekday_data)
                    
                    # Plot normal distribution curve
                    x = np.arange(PCT_MIN, PCT_MAX + BIN_WIDTH, BIN_WIDTH)
                    y = norm.pdf(x, mu, sigma) * len(hour_weekday_data) * BIN_WIDTH
                    ax.plot(
                        x,
                        y,
                        'r-',
                        linewidth=1.5,
                        label=f'Normal: μ={mu:.2f}%, σ={sigma:.2f}%'
                    )
                    
                    # Add statistics text box
                    stats_text = f'μ: {mu:.2f}%\nσ: {sigma:.2f}%\nn: {len(hour_weekday_data)}'
                    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                            verticalalignment='top', 
                            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.7),
                            fontsize=7)
                else:
                    # Not enough data for normal fit
                    stats_text = f'n: {len(hour_weekday_data)}\n(insufficient for fit)'
                    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                            verticalalignment='top', 
                            bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.7),
                            fontsize=7)
            else:
                # No data for this hour-weekday combination
                ax.text(0.5, 0.5, 'No Data', transform=ax.transAxes, 
                        ha='center', va='center', fontsize=12, 
                        bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.7))
            
            # Formatting
            ax.set_title(f'{weekday_name} {hour_label}', fontweight='bold', fontsize=10)
            
            # Only add labels to edge plots to avoid clutter
            if hour_idx == len(trading_hours) - 1:  # Bottom row
                ax.set_xlabel('Hourly % Change', fontsize=8)
            if weekday_idx == 0:  # Left column
                ax.set_ylabel('Frequency', fontsize=8)
            
            ax.grid(True, alpha=0.3)
            ax.set_xlim(PCT_MIN, PCT_MAX)
            
            # Smaller tick labels
            ax.tick_params(axis='both', which='major', labelsize=7)
            
            print(f"  Generated: {weekday_name} {hour_label} (n={len(hour_weekday_data)})")
    
    plt.tight_layout()
    plt.savefig('MASTEK_30_hourly_distributions.png', dpi=300, bbox_inches='tight')
    print(f"\nSaved 30 individual distributions as: MASTEK_30_hourly_distributions.png")
    plt.close()
    
    return True

def perform_statistical_tests(df):
    """Perform statistical tests to compare weekdays."""
    print("\n" + "="*60)
    print("STATISTICAL TESTS")
    print("="*60)
    
    # Prepare data for testing
    weekday_data = []
    weekday_labels = []
    
    for weekday in range(5):
        data = df[df['weekday'] == weekday]['pct_change']
        data = data[(data >= PCT_MIN) & (data <= PCT_MAX)]
        weekday_data.append(data.values)
        weekday_labels.append(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'][weekday])
    
    # Kruskal-Wallis test (non-parametric ANOVA)
    try:
        h_stat, p_value = stats.kruskal(*weekday_data)
        print(f"\nKruskal-Wallis Test:")
        print(f"  H-statistic: {h_stat:.4f}")
        print(f"  p-value: {p_value:.6f}")
        if p_value < 0.05:
            print("  Result: Significant difference between weekdays (p < 0.05)")
        else:
            print("  Result: No significant difference between weekdays (p >= 0.05)")
    except Exception as e:
        print(f"Kruskal-Wallis test failed: {e}")
    
    # Normality tests for each weekday
    print(f"\nNormality Tests (Shapiro-Wilk):")
    for i, (data, label) in enumerate(zip(weekday_data, weekday_labels)):
        if len(data) > 3:  # Shapiro-Wilk needs at least 3 observations
            try:
                # Use a sample if data is too large (Shapiro-Wilk limit is 5000)
                test_data = data[:5000] if len(data) > 5000 else data
                stat, p_val = stats.shapiro(test_data)
                print(f"  {label}: W={stat:.4f}, p={p_val:.6f}", end="")
                if p_val < 0.05:
                    print(" (Non-normal)")
                else:
                    print(" (Normal)")
            except Exception as e:
                print(f"  {label}: Test failed ({e})")

def create_hourly_patterns_analysis(df):
    """Analyze patterns by hour of day."""
    print("\n" + "="*60)
    print("HOURLY PATTERNS ANALYSIS")
    print("="*60)
    
    # Create hour-by-hour statistics
    hourly_stats = df.groupby('hour')['pct_change'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(4)
    
    print("\nHour-by-Hour Statistics:")
    print(hourly_stats)
    
    # Create hourly pattern plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # Plot 1: Average hourly returns
    hourly_means = df.groupby('hour')['pct_change'].mean()
    ax1.bar(hourly_means.index, hourly_means.values, alpha=0.7, color='skyblue', edgecolor='black')
    ax1.set_title('Average Hourly Returns by Hour of Day', fontweight='bold')
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Average Hourly Return (%)')
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.7)
    
    # Plot 2: Hourly volatility (standard deviation)
    hourly_std = df.groupby('hour')['pct_change'].std()
    ax2.bar(hourly_std.index, hourly_std.values, alpha=0.7, color='lightcoral', edgecolor='black')
    ax2.set_title('Hourly Volatility by Hour of Day', fontweight='bold')
    ax2.set_xlabel('Hour of Day')
    ax2.set_ylabel('Standard Deviation (%)')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('MASTEK_hourly_patterns.png', dpi=300, bbox_inches='tight')
    print(f"\nSaved hourly patterns plot as: MASTEK_hourly_patterns.png")
    plt.close()

def save_results_to_csv(df, stats_df, detailed_stats_df):
    """Save analysis results to CSV files."""
    
    # Save processed data
    df.to_csv('MASTEK_hourly_processed_data.csv', index=False)
    print(f"Saved processed hourly data to: MASTEK_hourly_processed_data.csv")
    
    # Save weekday statistics
    stats_df.to_csv('MASTEK_hourly_weekday_statistics.csv', index=False)
    print(f"Saved weekday statistics to: MASTEK_hourly_weekday_statistics.csv")
    
    # Save detailed 30 hour-weekday statistics
    detailed_stats_df.to_csv('MASTEK_30_hourly_detailed_statistics.csv', index=False)
    print(f"Saved 30 detailed hour-weekday statistics to: MASTEK_30_hourly_detailed_statistics.csv")
    
    # Create summary by weekday and hour
    hourly_weekday_summary = df.groupby(['weekday_name', 'hour'])['pct_change'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(4)
    hourly_weekday_summary.to_csv('MASTEK_hourly_weekday_summary.csv')
    print(f"Saved hourly weekday summary to: MASTEK_hourly_weekday_summary.csv")

def main():
    """Main execution function."""
    # File path to the hourly data
    csv_file = r"D:\Trading Strategies\historical_data_downloader\MASTEK_1h_full_historical.csv"
    
    print("MASTEK Hourly Distribution Analysis")
    print("=" * 50)
    
    # Load and process data
    df = load_and_process_hourly_data(csv_file)
    
    # Calculate statistics by weekday
    stats_df = calculate_statistics_by_weekday(df)
    
    # Calculate detailed statistics for all 30 hour-weekday combinations
    detailed_stats_df = calculate_detailed_hourly_statistics(df)
    
    # Create 30 individual hourly distribution plots
    create_hourly_weekday_distributions(df)
    
    # Perform statistical tests
    perform_statistical_tests(df)
    
    # Analyze hourly patterns
    create_hourly_patterns_analysis(df)
    
    # Save results
    save_results_to_csv(df, stats_df, detailed_stats_df)
    
    print("\n" + "="*80)
    print("30 HOUR-WEEKDAY DISTRIBUTION ANALYSIS COMPLETE")
    print("="*80)
    print("Files generated:")
    print("- MASTEK_30_hourly_distributions.png (30 individual distributions)")
    print("- MASTEK_hourly_patterns.png (hourly patterns)")
    print("- MASTEK_hourly_processed_data.csv (raw processed data)")
    print("- MASTEK_hourly_weekday_statistics.csv (weekday summaries)")
    print("- MASTEK_30_hourly_detailed_statistics.csv (30 detailed combinations)")
    print("- MASTEK_hourly_weekday_summary.csv (hour-weekday summary)")
    print(f"\n🎯 SUCCESS: Generated 30 individual hour-weekday distributions!")
    print("   - 5 weekdays × 6 trading hours = 30 unique distributions")
    print("   - Each shows frequency vs % change for that specific hour-weekday")

if __name__ == "__main__":
    main()
