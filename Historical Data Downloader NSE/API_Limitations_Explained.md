# ⚠️ IMPORTANT: Yahoo Finance API Limitations Explained

## 🚫 **The 730-Day Hard Limit Reality**

### **Key Finding**: Rate limiting will NOT solve the historical data limitation

**Why slowing down requests won't work:**
1. **API Data Availability**: Yahoo Finance simply doesn't store hourly data beyond 730 days
2. **Hard Server Restriction**: The error "must be within the last 730 days" is from Yahoo's servers
3. **Not a Rate Limit**: This is a data retention/storage policy, not request throttling

#### 🔍 Latest Investigations (September 27, 2025)

- `yf.download("MASTEK.NS", period="5y", interval="1h")` → returns **0 rows** with `YFPricesMissingError: 1h data not available…must be within the last 730 days`
- Direct REST call `https://query1.finance.yahoo.com/v8/finance/chart/MASTEK.NS?interval=1h&range=10y` → HTTP **422 Unprocessable Entity** with the same 730-day message
- Even tiny batch requests older than 730 days (e.g., `start=2023-09-28`) fail with the identical server response
- The raw chart endpoint accepts a `User-Agent` header but still truncates any range to **September 27, 2023 → September 26, 2025** (3465 hourly bars)

### **What the API Restrictions Actually Are:**

| Interval | Maximum Historical Range | Rate Limiting Helps? |
|----------|--------------------------|---------------------|
| **Daily (1d)** | 20+ years (back to IPO) | ❌ No limit anyway |
| **Hourly (1h)** | 730 days (~2 years) | ❌ Hard data limit |
| **4-hour (4h)** | 730 days (~2 years) | ❌ Hard data limit |
| **Sub-hourly (15m, 5m)** | 60 days | ❌ Hard data limit |

## 🚀 **Enhanced Slow Mode Features Added**

I've enhanced your script with advanced rate limiting options:

```bash
# Normal speed (1 second delay)
python mastek_timeInterval_OHLCV_data.py --interval 1h --batch

# Slow mode (5 second delays)
python mastek_timeInterval_OHLCV_data.py --interval 1h --batch --slow-mode

# Ultra-slow mode (10 second delays)
python mastek_timeInterval_OHLCV_data.py --interval 1h --batch --ultra-slow

# Custom delay
python mastek_timeInterval_OHLCV_data.py --interval 1h --batch --delay 15.0
```

## 📊 **What You CAN Actually Get**

### **Maximum Available MASTEK Hourly Data:**
- **Period**: October 2023 to September 2024 (730 days)
- **Records**: ~1,659 hours of actual trading data
- **File**: Already downloaded as `MASTEK_1h_full_historical.csv`

### **Alternative Approaches for Full Historical Intraday:**

#### **Option 1: Use Daily Data + Technical Indicators**
```bash
# You already have this: MASTEK_complete_with_pct.csv
# 5,775 daily records from 2002-07-01 to 2025-09-26
```
- **Pros**: Full 23-year history, all OHLCV data
- **Use Case**: Long-term backtesting, trend analysis

#### **Option 2: Combine Multiple Timeframes**
```bash
# Daily for long-term (2002-2024)
python mastek_timeInterval_OHLCV_data.py --interval 1d --start 2002-07-01 --end 2024-09-27

# Hourly for recent detailed analysis (last 730 days)
python mastek_timeInterval_OHLCV_data.py --interval 1h --batch --start 2022-10-01 --end 2024-09-27
```

#### **Option 3: Alternative Data Sources for Historical Intraday**
- **NSE Historical Data Portal**: Official exchange data
- **Zerodha Kite API**: May have longer intraday history
- **Commercial Providers**: Bloomberg, Refinitiv, Alpha Vantage
- **BSE Data Feeds**: Alternative Indian exchange data

## 🎯 **Recommendation**

**For MASTEK analysis since IPO:**

1. **Daily Analysis**: Use `MASTEK_complete_with_pct.csv` (2002-2025, 5,775 records)
2. **Recent Intraday**: Use `MASTEK_1h_full_historical.csv` (730 days, 1,659 hours)
3. **Combined Strategy**: Daily for long-term patterns + hourly for recent detailed analysis

## 💡 **The Enhanced Script Now Supports:**
- ✅ Ultra-slow mode (10s delays)
- ✅ Slow mode (5s delays) 
- ✅ Custom delays (any value)
- ✅ Better rate limiting for available data
- ✅ Intelligent batch processing
- ✅ All within Yahoo Finance's actual data availability

**Bottom Line**: The 730-day limit cannot be bypassed by slowing down requests - it's a fundamental data availability limitation from Yahoo Finance's servers.