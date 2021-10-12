import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/bandwidth.csv?start={}&end={}"
COLLECTION = "bandwidth"


class BandwidthEntry:
    def __init__(
        self,
        date: str,
        advbw: float,
        bwhist: float,
    ):
        try:
            self.date = dup.parse(date)
            self.advbw = float(advbw) if advbw else None
            self.bwhist = float(bwhist) if bwhist else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.date and self.advbw and self.bwhist

    def serialize(self) -> dict:
        return {
            "date": self.date,
            "advbw": self.advbw,
            "bwhist": self.bwhist,
        }


def ingest(entries: list[BandwidthEntry], init=False):
    return meta.ingest(
        entries=entries,
        collection=COLLECTION,
        filter_fields=["date"],
        init=init,
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=3, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return BandwidthEntry
