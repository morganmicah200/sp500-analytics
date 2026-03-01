import requests
import pandas as pd
from config import FRED_API_KEY

def fetch_economic_data():
    """
    Fetches the last 100 observations for three key economic indicators
    from the FRED API and returns them as a pandas DataFrame.
    
    Indicators:
    - FEDFUNDS: Federal Funds Rate (interest rates set by the Fed)
    - CPIAUCSL: Consumer Price Index (measure of inflation)
    - UNRATE: Unemployment Rate
    """
    
    # Dictionary of FRED series IDs and their human readable names
    indicators = {
        "FEDFUNDS": "Federal Funds Rate",
        "CPIAUCSL": "Consumer Price Index",
        "UNRATE": "Unemployment Rate"
    }
    
    all_records = []
    
    for series_id, name in indicators.items():
        print(f"Fetching {name} from FRED...")
        
        # API endpoint and parameters for each indicator
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,      # the indicator code
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "limit": 100,                # fetch last 100 recent observations
            "sort_order": "desc"         # most recent first
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        # Check if the response contains valid data
        if "observations" not in data:
            print(f"Error fetching {name}:", data)
            continue
        
        # Loop through observations and skip missing values marked as "."
        for obs in data["observations"]:
            if obs["value"] != ".":
                all_records.append({
                    "indicator_name": name,
                    "indicator_date": obs["date"],
                    "value": float(obs["value"])
                })
    
    # Convert all records into a single pandas DataFrame
    df = pd.DataFrame(all_records)
    print(f"Successfully fetched {len(df)} economic records")
    return df

if __name__ == "__main__":
    df = fetch_economic_data()
    if df is not None:
        print(df.head(10))