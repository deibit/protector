import sys
from datetime import datetime
from typing import Union
from dateutil.relativedelta import relativedelta

import pandas
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt

from scipy.stats.distributions import norm
from scipy.stats.distributions import poisson

from detector.utils.aux import get_dataframe, get_last_day


register_matplotlib_converters()

MAX_MODEL_COUNTRIES = 50
TIME_INTERVAL_DAYS = 7
TOP_COUNTRIES_INTERVAL = 1
today = datetime.now()
week = relativedelta(days=7)


def top_countries(
    top_df: pandas.DataFrame, interval: int = TOP_COUNTRIES_INTERVAL
) -> list[str]:
    """Return a list with the MAX_MODEL_COUNTRIES top users countries

    Args:
        df (pandas.Dataframe): The dataframe to operated with.
        interval (int, optional): Number of days to be considered. Defaults to TOP_COUNTRIES_INTERVAL.

    Returns:
        list[str]: A list with MAX_MODEL_COUNTRIES countries as strings (ISO codes)
    """

    end_date = get_last_day()
    start_date = end_date - relativedelta(days=interval)

    top_df = top_df.loc[[start_date, end_date]]

    top_countries = (
        top_df.sum().sort_values().tail(MAX_MODEL_COUNTRIES).index.values.tolist()
    )
    top_countries.sort()
    return top_countries


def get_trends(df) -> tuple[pandas.Series]:
    countries = top_countries(df)
    trend_df = df.filter(countries)

    def diff(users) -> Union[None, float]:
        try:
            a, b = (float(users[0]), float(users[1]))
            if not b:
                return None
            return a / b
        except:
            return None

    trends = pandas.Series(dtype="float64")
    iter = trend_df.iteritems()
    for country, row in iter:
        dr = pandas.concat([row, row.shift(periods=TIME_INTERVAL_DAYS)], axis=1).apply(
            diff, axis=1
        )
        dr.name = country
        trends = pandas.concat([trends, dr], axis=1)

    # Clean NaN column/row
    trends = trends.iloc[TIME_INTERVAL_DAYS:, :].iloc[:, 1:]

    def norm_maxmin(series):
        loc, scale = norm.fit(series)
        return pandas.Series(
            {
                "max": norm.ppf(0.9999, loc, scale),
                "min": norm.ppf(1 - 0.9999, loc, scale),
            }
        )

    maxmin = trends.apply(norm_maxmin, axis=1)
    return (maxmin["max"], maxmin["min"])


def set_f_poisson(min_trend, max_trend):
    def poissoned(df, *args):
        current_date = df[0]
        comparison_date = df[1]
        sys.exit()
        return None

    return poissoned


df: pandas.DataFrame = get_dataframe(days=30)
df = df.pivot_table(index="date", columns="country", values="users")
df = df.reset_index()
df = df.resample("D", on="date").mean()

# for column in df:
#     df[column] = df[column].fillna(df[column].mean())

max_trend, min_trend = get_trends(df)
poisson_f = set_f_poisson(min_trend, max_trend)

# print(df)

for name, row in df.iteritems():
    t_range = pandas.concat([row, row.shift(periods=TIME_INTERVAL_DAYS)], axis=1)
    print(t_range)
    t_range = t_range.apply(
        poisson_f,
        axis=1,
        args=(name,),
    )
    sys.exit()
