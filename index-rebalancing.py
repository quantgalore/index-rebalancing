# -*- coding: utf-8 -*-
"""
Created in 2023

@author: Quant Galore
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests 

from datetime import datetime, timedelta

fmp_api_key = "0f1bb59009e6ef4747289a60586bde4f"
polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"

sp500_historical_constituents = pd.json_normalize(requests.get(f"https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey={fmp_api_key}").json())
sp500_historical_constituents["date"] = pd.to_datetime(sp500_historical_constituents["date"])
sp500_historical_constituents["removedTicker"] = sp500_historical_constituents["removedTicker"].replace("","N/A")
sp500_historical_constituents["addedSecurity"] = sp500_historical_constituents["addedSecurity"].replace("","N/A")
sp500_historical_constituents["announcement_date"] = sp500_historical_constituents["date"] - timedelta(days=5)

deletions = sp500_historical_constituents[sp500_historical_constituents["removedTicker"] != "N/A"].copy()
deletions = deletions[(deletions["date"] >= "2023-01-01")]

additions = sp500_historical_constituents[sp500_historical_constituents["addedSecurity"] != "N/A"].copy()
additions = additions[(additions["date"] >= "2023-01-01")]

addition_tickers = additions["symbol"].values
deletion_tickers = deletions["symbol"].values

deletion_list = []

for deletion in deletion_tickers:
    
    deletion_data = deletions[deletions["symbol"] == deletion].copy()
    deletion_announcement_date = deletion_data["announcement_date"].iloc[0].strftime("%Y-%m-%d")
    deletion_effective_date = (deletion_data["date"].iloc[0]).strftime("%Y-%m-%d")
    
    thirty_days_later = (deletion_data["announcement_date"].iloc[0] + timedelta(days = 30)).strftime("%Y-%m-%d")
    
    start_date = deletion_announcement_date
    
    try:
        deleted_underlying = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{deletion}/range/1/day/{start_date}/{thirty_days_later}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")     
    except Exception as error:
        print(error)
        continue
    
    deleted_underlying.index = pd.to_datetime(deleted_underlying.index, unit = "ms", utc = True).tz_convert("America/New_York")
    deleted_underlying["returns"] = deleted_underlying["c"].pct_change().fillna(0)
    deleted_underlying["cumulative_returns"] = deleted_underlying["returns"].cumsum()
    
    effective_price = deleted_underlying["o"].iloc[0]
    
    ticker_contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={deletion}&contract_type=put&as_of={start_date}&expired=false&limit=1000&apiKey={polygon_api_key}").json()["results"])
    try:
        expiration_date = ticker_contracts[ticker_contracts["expiration_date"] >= (deletion_data["date"].iloc[0] + timedelta(days = 25)).strftime("%Y-%m-%d")]["expiration_date"].iloc[0]
    except Exception as error:
        print(error)
        continue
    
    Put_Contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={deletion}&contract_type=put&expiration_date={expiration_date}&as_of={start_date}&expired=false&limit=1000&apiKey={polygon_api_key}").json()["results"])
    Put_Contracts["distance_from_price"] = abs(Put_Contracts["strike_price"] - effective_price)
    
    put = Put_Contracts[Put_Contracts["distance_from_price"] == Put_Contracts["distance_from_price"].min()]
    put_symbol = put["ticker"].iloc[0]
    
    try:
        put_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{put_symbol}/range/1/day/{start_date}/{expiration_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
    except Exception as error:
        print(error)
        continue
    
    
    put_ohlcv.index = pd.to_datetime(put_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")
    
    open_price = put_ohlcv["o"].iloc[0]
    last_price = put_ohlcv["c"].iloc[-1]
    
    trade_dataframe = pd.DataFrame([{"effective_date": deletion_effective_date, "open_price": open_price,
                                     "last_price": last_price, "ticker": deletion, 
                                     "reason": deletion_data["reason"].iloc[0]}])
    
    
    deletion_list.append(trade_dataframe)
    

full_deletion_data = pd.concat(deletion_list).sort_values(by = "effective_date", ascending = True).set_index("effective_date")
full_deletion_data = full_deletion_data[full_deletion_data["ticker"] != "SBNY"]
full_deletion_data["gross_pnl"] = full_deletion_data["last_price"] - full_deletion_data["open_price"]
full_deletion_data["capital"] = 1000 + (full_deletion_data["gross_pnl"].cumsum() * 100)

plt.figure(dpi = 200)
plt.xticks(rotation = 45)
plt.plot(full_deletion_data["capital"])
plt.show()

#

addition_list = []

for addition in addition_tickers:
    
    addition_data = additions[additions["symbol"] == addition].copy()
    addition_announcement_date = addition_data["announcement_date"].iloc[0].strftime("%Y-%m-%d")
    addition_effective_date = (addition_data["date"].iloc[0]).strftime("%Y-%m-%d")
    
    thirty_days_later = (addition_data["announcement_date"].iloc[0] + timedelta(days = 30)).strftime("%Y-%m-%d")
    
    start_date = addition_announcement_date
    
    try:
        added_underlying = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{addition}/range/1/day/{start_date}/{thirty_days_later}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")     
    except Exception as error:
        print(error)
        continue
    
    added_underlying.index = pd.to_datetime(added_underlying.index, unit = "ms", utc = True).tz_convert("America/New_York")
    added_underlying["returns"] = added_underlying["c"].pct_change().fillna(0)
    added_underlying["cumulative_returns"] = added_underlying["returns"].cumsum()
    
    effective_price = added_underlying["o"].iloc[0]
    
    ticker_contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={addition}&contract_type=call&as_of={start_date}&expired=false&limit=1000&apiKey={polygon_api_key}").json()["results"])
    try:
        expiration_date = ticker_contracts[ticker_contracts["expiration_date"] >= (addition_data["date"].iloc[0] + timedelta(days = 25)).strftime("%Y-%m-%d")]["expiration_date"].iloc[0]
    except Exception as error:
        print(error)
        continue
    
    call_Contracts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={addition}&contract_type=call&expiration_date={expiration_date}&as_of={start_date}&expired=false&limit=1000&apiKey={polygon_api_key}").json()["results"])
    call_Contracts["distance_from_price"] = abs(call_Contracts["strike_price"] - effective_price)
    
    call = call_Contracts[call_Contracts["distance_from_price"] == call_Contracts["distance_from_price"].min()]
    call_symbol = call["ticker"].iloc[0]
    
    try:
        call_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{call_symbol}/range/1/day/{start_date}/{expiration_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
    except Exception as error:
        print(error)
        continue
    
    
    call_ohlcv.index = pd.to_datetime(call_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")
    
    open_price = call_ohlcv["o"].iloc[0]
    last_price = call_ohlcv["c"].iloc[-1]
    
    trade_dataframe = pd.DataFrame([{"effective_date": addition_effective_date, "open_price": open_price,
                                     "last_price": last_price, "ticker": addition, 
                                     "reason": addition_data["reason"].iloc[0]}])
    
    
    addition_list.append(trade_dataframe)

full_addition_data = pd.concat(addition_list).sort_values(by = "effective_date", ascending = True).set_index("effective_date")
full_addition_data["gross_pnl"] = full_addition_data["last_price"] - full_addition_data["open_price"]
full_addition_data["capital"] = 1000 + (full_addition_data["gross_pnl"].cumsum() * 100)

plt.figure(dpi = 200)
plt.xticks(rotation = 45)
plt.plot(full_addition_data["capital"])
plt.show()
