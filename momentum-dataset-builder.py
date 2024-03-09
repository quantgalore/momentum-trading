# -*- coding: utf-8 -*-
"""
Created in 2024

@author: Quant Galore
"""

import requests
import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
tickers = pd.read_sql("SELECT * FROM all_stocks", con = engine)["ticker"].values

full_grouped_bars = pd.read_sql("SELECT * FROM grouped_bars", con = engine)
full_grouped_bars = full_grouped_bars.set_index("t")

##

trading_calendar = calendar.schedule(start_date =full_grouped_bars.index[0].strftime("%Y-%m-%d"), end_date = (datetime.today())).resample('M').first()["market_open"].dt.strftime("%Y-%m-%d")
last_trading_calendar = calendar.schedule(start_date = full_grouped_bars.index[0].strftime("%Y-%m-%d"), end_date = (datetime.today())).resample('M').last()["market_open"].dt.strftime("%Y-%m-%d")

first_trading_days = trading_calendar.values[13:]
last_trading_days = last_trading_calendar.values[13:]

dataset_list = []
times = []

lookback_period = 12

for first_day in first_trading_days:
    
    start_time = datetime.now()
    
    last_day = last_trading_days[np.where(first_trading_days==first_day)][0]
    historical_data = full_grouped_bars[full_grouped_bars.index < first_day].copy()
    day_data = full_grouped_bars[full_grouped_bars.index.date == pd.to_datetime(first_day).date()].copy()
    
    # monthly_data = historical_data.groupby("ticker").resample("M").sum(numeric_only=True).reset_index()  
    
    # Get the top 2000 stocks by notional volume ($ amount traded) for timessake
    day_data["notional_volume"] = day_data["vw"] * day_data["v"]
    day_data = day_data.sort_values("notional_volume", ascending=False).head(2000)
    
    trading_tickers = day_data["ticker"].sort_values().values
    
    momentum_data_list = []
    
    for ticker in trading_tickers:
        
        ticker_data = historical_data[historical_data["ticker"] == ticker].copy()        
        
        first_day_close = ticker_data['c'].resample('M').first()
        last_day_close = ticker_data['c'].resample('M').last()
        
        monthly_returns = round(((last_day_close - first_day_close) / first_day_close)*100,2)
        monthly_ticker_data = monthly_returns.tail(lookback_period)
        
        if len(monthly_ticker_data) < lookback_period:
            continue
        
        last_n_months_avg_return = monthly_ticker_data.median()
        last_n_months_avg_return_mean = monthly_ticker_data.mean()
        return_std = monthly_ticker_data.std()
        last_return = monthly_ticker_data.tail(1).iloc[0]
        return_before_last = monthly_ticker_data.tail(2).iloc[0]
        
        full_ticker_data = full_grouped_bars[full_grouped_bars["ticker"] == ticker].copy()
        trading_day_data = full_ticker_data[full_ticker_data.index.date == pd.to_datetime(first_day).date()]
        selling_day_data = full_ticker_data[full_ticker_data.index.date == pd.to_datetime(last_day).date()]
        
        if (len(trading_day_data) < 1) or (len(selling_day_data) < 1):
            continue
        
        momentum_dataframe = pd.DataFrame([{"date": pd.to_datetime(first_day),"ticker": ticker, "last_n_mo_avg_returns": last_n_months_avg_return,
                                            "last_n_mo_avg_returns_mean": last_n_months_avg_return_mean, "return_std": return_std, 
                                            "return_before_last_month": return_before_last, "last_month_return": last_return,
                                            "trade_price": trading_day_data["c"].iloc[0], "pct_change": round(((selling_day_data["c"].iloc[0] - trading_day_data["c"].iloc[0]) / trading_day_data["c"].iloc[0])*100,2)}])
        
        momentum_data_list.append(momentum_dataframe)

    momentum_data_original = pd.concat(momentum_data_list).sort_values(by = "last_n_mo_avg_returns", ascending = True)
    
    momentum_data = momentum_data_original.copy()
    dataset_list.append(momentum_data)
    
    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(first_trading_days==first_day)[0][0]/len(first_trading_days))*100,2)
    iterations_remaining = len(first_trading_days) - np.where(first_trading_days==first_day)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

full_dataset = pd.concat(dataset_list)

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
full_dataset.to_sql("momentum_dataset", con = engine, if_exists = "replace")