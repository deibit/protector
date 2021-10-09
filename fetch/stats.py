import csv
import sys
import time

import requests
import pymongo

from log import logger

USERS_STATS = (
    "https://metrics.torproject.org/userstats-relay-country.csv?start={}&end={} "
)

today = time.gmtime()


def download(stats_url):
    try:
        resp = requests.get(stats_url)

        if not resp.status_code == 200:
            logger.error("Error [%s] when downloading %s", resp.status_code, stats_url)
            sys.exit(-1)

        return resp.content

    except Exception as e:
        logger.exception("Exception when downloading %s", resp.status_code, stats_url)
        logger.exception(e)


def _pretty_print(obj):
    pass


if __name__ == "__main__":
    c = download(USERS_STATS.format("2021-10-05", "2021-10-07"))
    for l in c.decode("utf").split("\n"):
        print(l)
