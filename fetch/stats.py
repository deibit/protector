import sys

import requests
import celery

from dateutil.relativedelta import relativedelta
from dateutil.utils import today

from utils.log import logger
from fetch.models import modules
from detector import detectors
from utils.db import check_tables

app = celery.current_app
app.control.purge()


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


@app.task(ignore_result=True, name="fetch")
def fetch():
    check_tables()
    for model in modules.names:
        logger.info("Processing stats for model %s", model.__name__)
        last = model.last()

        if last:
            past = last["date"]
            # If we've got a last record then download +1 day past last record
            past = past + relativedelta(days=+1)
            from_date = "{}-{}-{}".format(past.year, past.month, past.day)

            now = today()
            to_date = "{}-{}-{}".format(now.year, now.month, now.day)

            logger.info(
                "Downloading stats file for %s from %s to %s",
                model.__name__,
                from_date,
                to_date,
            )
            c = download(model.URL.format(from_date, to_date))

        else:
            logger.info("Downloading complete stats file for %s", model.__name__)
            c = download(model.URL.split("?")[0])

        c = model.purify(c)
        if not c:
            logger.info("Nothing to update on %s", model.__name__)
        else:
            entries = [model.get_class()(*entry) for entry in c]
            model.ingest(entries)

    # Call detectors
    for detection in detectors.modules:
        logger.info(f"Calling {detection.__name__} detector")
        detection.get_detections()


if __name__ == "__main__":
    fetch()
