# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 11:48:03 2024

@author: quant
"""

import requests
import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

import xgboost
import pmdarima as pmd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import KFold, cross_validate

def Binarizer(value):
    
    if value > 0:
        return 1
    elif value <= 0:
        return 0

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')
original_dataset = pd.read_sql("SELECT * FROM momentum_dataset", con = engine)
tickers = pd.read_sql("SELECT * FROM all_stocks", con = engine)

dataset = original_dataset.copy().dropna().set_index("date")
dataset = dataset[dataset["ticker"].isin(tickers["ticker"].values)]

# sample selection
dataset = dataset[dataset.index >= "2022-07-01"]

dataset = dataset.drop("index", axis = 1)
dataset = pd.get_dummies(dataset)

target = "pct_change"

X_train = dataset.copy().drop(target, axis = 1)
Y_train = dataset[target].values
Y_train_class = dataset[target].apply(Binarizer).values

#

k_folds = KFold(n_splits = 5, shuffle = False)
rf_classification_scores = pd.DataFrame(cross_validate(estimator = RandomForestClassifier(), X=X_train, y=Y_train_class, cv=k_folds, scoring = ["accuracy", "precision", "recall", "f1", "roc_auc"]))
# Running the K-Fold validation on the regression model may take a long time depending on your processing power. 
rf_regression_scores = pd.DataFrame(cross_validate(estimator =  RandomForestRegressor(), X=X_train, y=Y_train.ravel(), cv=k_folds, scoring = ["neg_mean_absolute_error", "r2", "max_error"]))