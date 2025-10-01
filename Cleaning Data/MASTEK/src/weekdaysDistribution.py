"""
MASTEK Weekday Distribution Analysis

This script analyzes the MASTEK stock data to create 5 normal distributions
for each weekday (Monday-Friday), showing the frequency of percentage changes
on the X-axis and frequency on the Y-axis.
"""

import pandas as pd
import numpy as np
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

def load_and_process_data(csv_path):
    """Load CSV data and process dates and percentage changes."""
    print("Loading data...")
    df = pd.read_csv(csv_path)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Extract weekday (0=Monday, 1=Tuesday, ..., 4=Friday, 5=Saturday, 6=Sunday)
    df['weekday'] = df['date'].dt.dayofweek
    
    # Filter for weekdays only (0-4, Monday to Friday)
    df = df[df['weekday'] <= 4].copy()
    
    # Remove rows with NaN percentage changes
    df = df.dropna(subset=['pct_change'])

    # Keep percentage changes within requested range
    df = df[(df['pct_change'] >= PCT_MIN) & (df['pct_change'] <= PCT_MAX)].copy()
    
    # Create weekday names
    weekday_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday'}
    df['weekday_name'] = df['weekday'].map(weekday_names)
    
    print(f"Loaded {len(df)} trading days of data")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    
    return df

def calculate_statistics_by_weekday(df):
    """Calculate basic statistics for each weekday."""
    print("\n" + "="*60)
    print("WEEKDAY STATISTICS SUMMARY")
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
        print(f"  Count: {stats_dict['Count']}")
        print(f"  Mean: {stats_dict['Mean (%)']:.4f}%")
        print(f"  Std Dev: {stats_dict['Std Dev (%)']:.4f}%")
        print(f"  Range: {stats_dict['Min (%)']:.2f}% to {stats_dict['Max (%)']:.2f}%")
        print(f"  Skewness: {stats_dict['Skewness']:.4f}")
        print(f"  Kurtosis: {stats_dict['Kurtosis']:.4f}")
    
    return pd.DataFrame(stats_summary)

def create_weekday_distributions(df):
    """Create histogram and normal distribution plots for each weekday."""
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create subplot layout
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('MASTEK Stock: Percentage Change Distributions by Weekday', 
                 fontsize=16, fontweight='bold')
    
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    # Plot for each weekday
    for i, weekday in enumerate(range(5)):
        row = i // 3
        col = i % 3
        ax = axes[row, col]
        
        # Get data for this weekday within configured range
        weekday_data = df[df['weekday'] == weekday]['pct_change']
        weekday_data = weekday_data[(weekday_data >= PCT_MIN) & (weekday_data <= PCT_MAX)]
        weekday_name = weekday_names[weekday]
        
        # Create histogram
        n, bins, patches = ax.hist(
            weekday_data,
            bins=BINS,
            density=False,
            alpha=0.7,
            color=colors[i],
            edgecolor='black',
            linewidth=0.5,
        )
        
        # Fit normal distribution
        mu, sigma = norm.fit(weekday_data)
        
        # Plot normal distribution curve
        x = np.arange(PCT_MIN, PCT_MAX + BIN_WIDTH, BIN_WIDTH)
        y = norm.pdf(x, mu, sigma) * len(weekday_data) * BIN_WIDTH
        ax.plot(
            x,
            y,
            'r-',
            linewidth=2,
            label=f'Normal: μ={mu:.2f}%, σ={sigma:.2f}%'
        )
        
        # Formatting
        ax.set_title(f'{weekday_name}\n(n={len(weekday_data)})', 
                    fontweight='bold', fontsize=12)
        ax.set_xlabel('Percentage Change (%)', fontsize=10)
        ax.set_ylabel('Frequency', fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(PCT_MIN, PCT_MAX)
        
        # Add statistics text box
        stats_text = f'Mean: {mu:.2f}%\nStd: {sigma:.2f}%\nSkew: {stats.skew(weekday_data):.2f}'
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
               verticalalignment='top', bbox=dict(boxstyle='round', 
               facecolor='wheat', alpha=0.8), fontsize=8)
    
    # Remove the empty subplot
    axes[1, 2].remove()
    
    plt.tight_layout()
    return fig

def create_combined_distribution_plot(df):
    """Create a combined plot showing all weekday distributions on one chart."""
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    for i, weekday in enumerate(range(5)):
        weekday_data = df[df['weekday'] == weekday]['pct_change']
        weekday_data = weekday_data[(weekday_data >= PCT_MIN) & (weekday_data <= PCT_MAX)]
        weekday_name = weekday_names[weekday]
        
        # Fit normal distribution
        mu, sigma = norm.fit(weekday_data)
        
        # Create x range for smooth curve
        x = np.arange(PCT_MIN, PCT_MAX + BIN_WIDTH, BIN_WIDTH)
        y = norm.pdf(x, mu, sigma)
        
        # Plot normal distribution curve
        ax.plot(x, y, linewidth=2.5, label=f'{weekday_name} (μ={mu:.2f}%, σ={sigma:.2f}%)',
               color=colors[i])
        
        # Add vertical line for mean
        ax.axvline(mu, color=colors[i], linestyle='--', alpha=0.7, linewidth=1)
    
    ax.set_title('MASTEK: Normal Distribution Comparison by Weekday', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Percentage Change (%)', fontsize=12)
    ax.set_ylabel('Probability Density', fontsize=12)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(PCT_MIN, PCT_MAX)
    
    # Add overall statistics
    overall_mean = df['pct_change'].mean()
    ax.axvline(overall_mean, color='black', linestyle='-', alpha=0.8, linewidth=2,
              label=f'Overall Mean: {overall_mean:.2f}%')
    
    plt.tight_layout()
    return fig

def perform_statistical_tests(df):
    """Perform statistical tests to compare weekday distributions."""
    print("\n" + "="*60)
    print("STATISTICAL TESTS")
    print("="*60)
    
    weekday_data = {}
    for weekday in range(5):
        weekday_data[weekday] = df[df['weekday'] == weekday]['pct_change']
    
    # Perform ANOVA test
    f_stat, p_value = stats.f_oneway(*weekday_data.values())
    print(f"\nANOVA Test (comparing all weekdays):")
    print(f"F-statistic: {f_stat:.4f}")
    print(f"P-value: {p_value:.6f}")
    
    if p_value < 0.05:
        print("Result: Significant difference between weekdays (p < 0.05)")
    else:
        print("Result: No significant difference between weekdays (p >= 0.05)")
    
    # Perform normality tests for each weekday
    print(f"\nNormality Tests (Shapiro-Wilk):")
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    for weekday in range(5):
        data = weekday_data[weekday]
        if len(data) > 5000:  # Shapiro-Wilk has limitations for large samples
            print(f"{weekday_names[weekday]}: Sample too large for Shapiro-Wilk test (n={len(data)})")
        else:
            stat, p_val = stats.shapiro(data)
            print(f"{weekday_names[weekday]}: W={stat:.4f}, p={p_val:.6f}")

def main():
    """Main function to run the analysis."""
    
    # File path
    csv_path = r"d:\Trading Strategies\historical_data_downloader\MASTEK_complete_with_pct.csv"
    
    print("MASTEK Weekday Distribution Analysis")
    print("=" * 50)
    
    try:
        # Load and process data
        df = load_and_process_data(csv_path)
        
        # Calculate statistics
        stats_df = calculate_statistics_by_weekday(df)
        
        # Create individual weekday distribution plots
        fig1 = create_weekday_distributions(df)
        fig1.savefig(r"d:\Trading Strategies\Cleaning Data\weekday_distributions.png", 
                     dpi=300, bbox_inches='tight')
        print("Individual weekday distributions saved as 'weekday_distributions.png'")
        
        # Create combined distribution plot
        fig2 = create_combined_distribution_plot(df)
        fig2.savefig(r"d:\Trading Strategies\Cleaning Data\combined_distributions.png", 
                     dpi=300, bbox_inches='tight')
        print("Combined distributions saved as 'combined_distributions.png'")
        
        plt.close('all')  # Close all figures to free memory
        
        # Perform statistical tests
        perform_statistical_tests(df)
        
        # Save statistics to CSV
        output_path = r"d:\Trading Strategies\Cleaning Data\weekday_statistics_trimmed.csv"
        stats_df.to_csv(output_path, index=False)
        print(f"\nStatistics saved to: {output_path}")
        
        print("\nAnalysis complete!")
        
    except FileNotFoundError:
        print(f"Error: Could not find the file {csv_path}")
        print("Please check the file path and try again.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()