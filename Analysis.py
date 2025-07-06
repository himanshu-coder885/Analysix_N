# ⬇️ Required Libraries
import pandas as pd
import datetime
import yfinance as yf
from ta.trend import PSARIndicator

# ⬇️ Config
diff_low_sar_perc = 5  # 5% allowed difference

# ⬇️ Load stock list CSV from local path (same folder or give full path)
df = pd.read_csv("ind_nifty500list.csv")  # Make sure this file exists in your working directory
df["Ticker"] = df["Symbol"] + ".NS"

# ⬇️ Function: Aggregate n-day candlestick
def aggregate_n_day(data, days):
    resample_rule = f'{days}D'
    ticker = data.columns.levels[1][0]
    data_nd = data.resample(resample_rule).agg({
        ('Open', ticker): 'first',
        ('High', ticker): 'max',
        ('Low', ticker): 'min',
        ('Close', ticker): 'last',
        ('Volume', ticker): 'sum'
    })
    return data_nd

# ⬇️ Function: Add Parabolic SAR column
def add_parabolic_sar(data, ticker):
    psar = PSARIndicator(
        high=data[('High', ticker)],
        low=data[('Low', ticker)],
        close=data[('Close', ticker)],
        step=0.02,
        max_step=0.2
    )
    data[('SAR', ticker)] = psar.psar()
    return data

# ⬇️ Create result columns
df['3D_condition'] = 0
df['4D_condition'] = 0
df['5D_condition'] = 0
df['Error'] = 0

# Optional: Test with fewer stocks during debugging
# df = df.sample(n=10, random_state=42)

# ⬇️ Loop over each stock
for index, row in df.iterrows():
    ticker = row['Ticker']
    print(f"Processing {ticker}...")

    try:
        data = yf.download(ticker, interval="1d", period="max", progress=False)
        if data.empty:
            print(f"No data for {ticker}")
            df.at[index, 'Error'] = 1
            continue

        data.index = pd.to_datetime(data.index)

        # Convert to MultiIndex
        if not isinstance(data.columns, pd.MultiIndex):
            data.columns = pd.MultiIndex.from_tuples([(col, ticker) for col in data.columns])

        # Loop through 3D, 4D, 5D aggregation
        for days, column_name in zip([3, 4, 5], ['3D_condition', '4D_condition', '5D_condition']):
            data_nd = aggregate_n_day(data, days)
            if data_nd.empty:
                print(f"No aggregated data for {ticker} ({days}D). Skipping.")
                continue

            data_nd = add_parabolic_sar(data_nd, ticker)

            low_col = data_nd[('Low', ticker)]
            sar_col = data_nd[('SAR', ticker)]

            condition = (low_col > sar_col) & ((low_col - sar_col) <= (diff_low_sar_perc / 100) * low_col)
            df.at[index, column_name] = int(condition.iloc[-1]) if not condition.empty else 0

    except Exception as e:
        print(f"Error for {ticker}: {e}")
        df.at[index, 'Error'] = 1

# ⬇️ Save results to Excel file in same folder
today_date = datetime.datetime.today().strftime('%Y-%m-%d')
diff_str = f"{diff_low_sar_perc}pct"
file_path = f"{today_date}_{diff_str}.xlsx"

df.to_excel(file_path, index=False, engine='openpyxl')
print(f"✅ File saved successfully as {file_path}")
