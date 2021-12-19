from typing import Union
from dateutil.relativedelta import relativedelta

import pandas

from scipy.stats.distributions import norm
from scipy.stats.distributions import poisson

from detector.utils.aux import get_dataframe, get_last_day

from utils.log import logger
from utils.db import insertions, connect


MAX_MODEL_COUNTRIES = 50
TIME_INTERVAL_DAYS = 7
DETECTION_DAYS_INTERVAL = 10
TOP_COUNTRIES_INTERVAL = 1
TOP = 0.9999
FLOOR = 1 - TOP


def insert_alert(date, country, users, trend, max, min, detector="danezis") -> None:
    c = connect()
    cursor = c.cursor()
    cursor.execute(
        insertions["alerts"],
        (
            date,
            country,
            users,
            trend,
            max,
            min,
            detector,
        ),
    )
    c.commit()


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
    """Return a tuple with a series representing the trends

    Args:
        df (pandas.Dataframe): The dataframe to operated with.

    Returns:
        tuple[pandas.Series]: A series with the trends
    """
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
                "max": norm.ppf(TOP, loc, scale),
                "min": norm.ppf(FLOOR, loc, scale),
            }
        )

    maxmin = trends.apply(norm_maxmin, axis=1)
    return (maxmin["max"], maxmin["min"])


def set_f_poisson(min_trend, max_trend):
    def poissoned(p_df, name):
        try:
            current_date = p_df.iloc[[0]]
            comparison_date = p_df.iloc[[1]]

            current_date_str = current_date.name.date()

            g_min = min_trend[p_df.name]
            g_max = max_trend[p_df.name]

            min_range = g_min * poisson.ppf(FLOOR, comparison_date)
            max_range = g_max * poisson.ppf(TOP, comparison_date)

            the_value = current_date.values[0]

            if the_value < min_range:
                logger.info(
                    "[%s] down detected in %s with %s for a min of %s",
                    current_date_str,
                    name,
                    the_value,
                    min_range,
                )
                insert_alert(
                    current_date.name,
                    name,
                    the_value,
                    "DOWN",
                    max_range[0],
                    min_range[0],
                )

            if the_value > max_range:
                logger.info(
                    "[%s] up detected in %s with %s for a max of %s",
                    current_date_str,
                    name,
                    the_value,
                    max_range,
                )
                insert_alert(
                    current_date.name, name, the_value, "UP", max_range[0], min_range[0]
                )

        except KeyError:
            return pandas.Series({"country": name, "min": None, "max": None})

    return poissoned


def get_detections() -> None:
    df: pandas.DataFrame = get_dataframe(days=DETECTION_DAYS_INTERVAL)
    df = df.pivot_table(index="date", columns="country", values="users")

    for column in df:
        df[column] = df[column].fillna(df[column].mean())

    max_trend, min_trend = get_trends(df)
    poisson_f = set_f_poisson(min_trend, max_trend)

    for name, row in df.iteritems():
        t_range = pandas.concat([row, row.shift(periods=TIME_INTERVAL_DAYS)], axis=1)
        t_range = t_range.apply(
            poisson_f,
            axis=1,
            args=(name,),
        )


if __name__ == "__main__":
    get_detections()
