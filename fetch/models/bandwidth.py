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
        return self.__dict__


def ingest(entries: list[BandwidthEntry]):
    return meta.ingest(
        entries=entries,
        collection_name=COLLECTION,
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=3, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return BandwidthEntry
