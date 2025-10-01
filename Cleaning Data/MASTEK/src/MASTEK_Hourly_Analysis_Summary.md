# MASTEK Hourly Distribution Analysis Results

## 📊 **Analysis Overview**
- **Data Source**: MASTEK_1h_full_historical.csv
- **Time Period**: October 4, 2023 to September 26, 2024
- **Total Hours**: 1,659 hourly data points
- **Trading Hours**: 03:45:00 to 09:45:00 (Indian market hours)
- **Analysis Type**: Hourly percentage changes by weekday

## 🔍 **Key Findings**

### **Weekday Statistics Summary:**

| Weekday | Hours | Mean Return | Std Dev | Range | Skewness | Kurtosis |
|---------|-------|-------------|---------|-------|----------|----------|
| **Monday** | 315 | -0.050% | 0.862% | -3.82% to 3.85% | -0.012 | 3.955 |
| **Tuesday** | 336 | -0.083% | 0.748% | -3.19% to 3.27% | 0.227 | 3.933 |
| **Wednesday** | 336 | -0.035% | 0.848% | -3.61% to 6.09% | 1.022 | 9.260 |
| **Thursday** | 343 | **+0.051%** | 1.068% | -3.91% to **10.97%** | **4.258** | **40.031** |
| **Friday** | 329 | -0.054% | 0.879% | -4.22% to 3.73% | 0.679 | 4.482 |

### **Statistical Insights:**

#### 🎯 **Performance by Weekday:**
- **Best Performing**: Thursday (only positive average return: +0.051%)
- **Most Volatile**: Thursday (highest std dev: 1.068%)
- **Most Stable**: Tuesday (lowest std dev: 0.748%)
- **Largest Single Gain**: Thursday (+10.97%)
- **Largest Single Loss**: Friday (-4.22%)

#### 📈 **Distribution Characteristics:**
- **Most Normal**: Monday (skewness ≈ 0, kurtosis ≈ 4)
- **Most Skewed**: Thursday (highly right-skewed: 4.258)
- **Highest Kurtosis**: Thursday (fat tails: 40.031)
- **Most Symmetric**: Monday (skewness: -0.012)

#### ⏰ **Hourly Patterns:**
- **Most Volatile Hour**: 03:45 (market opening, std dev: 1.436%)
- **Most Stable Hour**: 09:45 (market closing, std dev: 0.430%)
- **Best Performing Hour**: 05:45 (+0.086% average)
- **Worst Performing Hour**: 03:45 (-0.154% average)

## 🧪 **Statistical Tests Results**

### **Kruskal-Wallis Test:**
- **H-statistic**: 3.907
- **p-value**: 0.419
- **Result**: **No significant difference** between weekdays (p ≥ 0.05)

### **Normality Tests (Shapiro-Wilk):**
- **All weekdays**: Non-normal distributions (p < 0.05)
- **Thursday**: Most non-normal (W = 0.701)
- **Tuesday**: Closest to normal (W = 0.932)

## 📋 **Trading Implications**

### **Risk Management:**
- **Thursday**: Highest risk-reward ratio (biggest gains but most volatile)
- **Tuesday**: Most predictable patterns (lowest volatility)
- **Opening Hour (03:45)**: Highest volatility - avoid large positions

### **Opportunity Identification:**
- **Thursday Sessions**: Higher probability of significant moves
- **Morning Hours (05:45)**: Slight positive bias
- **Closing Hour (09:45)**: Lower volatility for stable positions

## 📁 **Generated Files:**
1. **MASTEK_hourly_weekday_distributions.png** - Distribution plots by weekday
2. **MASTEK_hourly_patterns.png** - Hour-by-hour analysis
3. **MASTEK_hourly_processed_data.csv** - Full processed dataset
4. **MASTEK_hourly_weekday_statistics.csv** - Summary statistics
5. **MASTEK_hourly_weekday_summary.csv** - Detailed hour-by-weekday breakdown

## 💡 **Key Differences from Daily Analysis:**
- **Hourly data** shows much smaller percentage changes (typically < 1%)
- **Intraday patterns** reveal opening/closing hour effects not visible in daily data
- **Higher frequency** allows for more granular pattern recognition
- **Thursday anomaly** more pronounced in hourly data than daily data

## 🎯 **Conclusion:**
The hourly analysis reveals that while weekdays don't show statistically significant differences overall, **Thursday stands out** with the highest average returns, volatility, and extreme movements. The **opening hour** consistently shows the highest volatility across all weekdays, while the **closing hour** is the most stable.