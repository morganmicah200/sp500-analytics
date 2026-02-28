import requests
import pandas as pd
from config import ALPHA_VANTAGE_API_KEY

def fetch_sp500_data():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol" : "SPY",
        "outputsize" : "compact",
        "apikey": ALPHA_VANTAGE_API_KEY
    }

    print("Fetching S&P 500 data from Alpha Vantage...")
    response = requests.get(url, params=params)
    data = response.json()
    
    if "Time Series (Daily)" not in data:
        print("Error fetching data:", data)
        return None
    
    time_series = data["Time Series (Daily)"]
    records = []
    
    for date, values in time_series.items():
        records.append({
            "trade_date": date,
            "open_price": float(values["1. open"]),
            "high_price": float(values["2. high"]),
            "low_price": float(values["3. low"]),
            "close_price": float(values["4. close"]),
            "volume": int(values["5. volume"])
        })
    
    df = pd.DataFrame(records)
    print(f"Successfully fetched {len(df)} records")
    return df

if __name__ == "__main__":
    df = fetch_sp500_data()
    if df is not None:
        print(df.head())