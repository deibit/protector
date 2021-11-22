import pandas
from dateutil.relativedelta import relativedelta

from utils.db import connect

LAST_DATE = "SELECT date FROM protector.users ORDER BY date DESC LIMIT 1;"
USERS = "SELECT date, country, users FROM protector.users WHERE date > %s AND country != '??' ORDER BY date DESC;"


def get_last_day():
    """Returns the last day in database stats

    Returns:
        datetime: Last day in database
    """
    c = connect()
    cursor = c.cursor()
    cursor.execute(LAST_DATE)
    last_date = cursor.fetchone()[0]
    return last_date


def get_dataframe(*, days=1):
    """Get a dataframe from database of 'days' back since last recorded day

    Args:
        days (int, optional): Number of days to be included. Defaults to 1.

    Returns:
        pandas.DataFrame: A Dataframe of days to last_day
    """
    relative = relativedelta(days=days)
    last_day = get_last_day()

    c = connect()
    cursor = c.cursor()

    time_slice = last_day - relative
    cursor.execute(
        USERS,
        (str(time_slice),),
    )
    i = cursor.fetchall()

    # Load main DataFrame
    df = pandas.DataFrame(i, columns=["date", "country", "users"])
    return df


def cap100(df: pandas.DataFrame, n_users: int = 100):
    """Filter countries with less than 100 users on every single day

    Args:
        df (pandas.DataFrame): The DataFrame to be processed
        n_users (int, optional): Minimun users to consider as filter. Defaults to 100.

    Returns:
        pandas.DataFrame: The processed dataframe after filtering
    """
    #
    df2 = df.copy(deep=True)
    df2 = df2.groupby("country").max()
    df2 = df2[df2.users > n_users]
    countries_more_than_100 = df2.index.values.tolist()

    # Filter less than 100 users
    df = df[df.country.isin(countries_more_than_100)]
    return df
