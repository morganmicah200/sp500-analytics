import requests
import pandas as pd
from config import ALPHA_VANTAGE_API_KEY

def fetch_sp500_data():
    """
    Fetches the last 100 days of S&P 500 (SPY) daily price data
    from the Alpha Vantage API and returns it as a pandas DataFrame.
    """
    
    # API endpoint and parameters for daily time series data
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "SPY",          # SPY is the S&P 500 ETF ticker
        "outputsize": "compact",  # compact returns last 100 data points
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    
    print("Fetching S&P 500 data from Alpha Vantage...")
    response = requests.get(url, params=params)
    data = response.json()
    
    # Check if the response contains valid data
    if "Time Series (Daily)" not in data:
        print("Error fetching data:", data)
        return None
    
    # Extract the time series and build a list of records
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
    
    # Convert list of records into a pandas DataFrame
    df = pd.DataFrame(records)
    print(f"Successfully fetched {len(df)} records")
    return df

if __name__ == "__main__":
    df = fetch_sp500_data()
    if df is not None:
        print(df.head())