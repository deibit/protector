import sys

from utils.db import connect
from utils.log import logger

from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from statsmodels.tsa.seasonal import STL
from sklearn.decomposition import PCA

import pandas


import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

MIN_USERS = 100

c = connect()

BRIDGES = "SELECT * FROM protector.bridges WHERE users > 200 AND date > {}"
USERS = "SELECT date,country,users FROM protector.users WHERE date > %s AND country != '??' ORDER BY date DESC"
ONIONSERVICES = "SELECT * FROM protector.bridges WHERE date > {}"

collections = [ONIONSERVICES, BRIDGES, USERS]

today = datetime.now()
rd_180 = relativedelta(days=179, weekday=MO(1))
rd_2y = relativedelta(years=2, weekday=MO(1))
rd_60d = relativedelta(days=60, weekday=MO(1))

cursor = c.cursor()
cursor.execute(USERS, (str(today - rd_2y),))
i = cursor.fetchall()

# Load main DataFrame
df = pandas.DataFrame(i, columns=["date", "country", "users"])


def cap100(df):
    # Copy and filter countries with less than 100 users on every single day
    df2 = df.copy(deep=True)
    df2 = df2.groupby("country").max()
    df2 = df2[df2.users > MIN_USERS]
    countries_more_than_100 = df2.index.values.tolist()

    # Filter less than 100 users
    df = df[df.country.isin(countries_more_than_100)]
    return df


def add_STL(df):
    # Apply STL to remove seasonality and adding columns of 'STL_trends' and 'STL_residuals'
    df3 = pandas.DataFrame()

    dfg = df.groupby("country")
    for name, group in dfg:
        try:
            group.set_index("date", inplace=True)
            group = group.resample("M").mean().ffill()
            res = STL(group["users"]).fit()
            group["residual"] = res.resid
            group["trend"] = res.trend
            # Restore country name
            group["country"] = name
            df3 = df3.append(group)
        except Exception as e:
            pass
            # logger.exception("Error when doing STL over %s", name)
    df3.reset_index(level=0, inplace=True)
    return df3


# PCA - select last 180 days records
start_date = today - rd_180
end_date = today
mask = (df["date"] > start_date) & (df["date"] <= end_date)
mask = df.loc[mask]
mask = mask.groupby("date")
# We need an array of (date(index), country_1, country_2...X users)
for name, group in mask:
    print(name)
# pca = PCA(n_components=180)
# pca = pca.fit(mask)
# print(pca)
