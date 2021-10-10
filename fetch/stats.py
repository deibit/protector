import sys
import time

import requests

from dateutil.relativedelta import relativedelta
from dateutil.utils import today

from log import logger
from models import users


USERSSTATS = (
    "https://metrics.torproject.org/userstats-relay-country.csv?start={}&end={}"
)


def download(stats_url: str) -> list[str]:
    try:
        logger.info("Downloading %s", stats_url)
        resp = requests.get(stats_url)

        if not resp.status_code == 200:
            logger.error("Error [%s] when downloading %s", resp.status_code, stats_url)
            sys.exit(-1)

        return resp.content.decode("utf").split("\n")

    except Exception as e:
        logger.exception(
            "Exception %s when downloading %s", resp.status_code, stats_url
        )
        logger.exception(e)


if __name__ == "__main__":
    init = False
    last = users.last()
    if last:
        past = last["date"]
        # If we've got a last record then download +1 day past last record
        past = past + relativedelta(days=+1)
        from_date = "{}-{}-{}".format(past.year, past.month, past.day)

        now = today()
        to_date = "{}-{}-{}".format(now.year, now.month, now.day)

        logger.info("Downloading stats file from %s to %s", from_date, to_date)
        c = download(USERSSTATS.format(from_date, to_date))

    else:
        logger.info("Downloading complete stats file")
        init = True
        c = download(USERSSTATS.split("?")[0])

    c = users.purify(c)
    if not c:
        logger.info("Nothing to update")
        sys.exit()
    users.ingest([users.UsersEntry(*user) for user in c], init=init)
