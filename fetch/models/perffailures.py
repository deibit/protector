import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/torperf-failures.csv?start={}&end={}"
COLLECTION = "perffailures"


class PerfFailuresEntry:
    def __init__(
        self,
        date: str,
        source: str,
        server: str,
        timeouts: str = "",
        failures: str = "",
    ):
        try:
            self.date = dup.parse(date)
            self.source: str = source
            self.server: str = server
            self.timeouts: float = float(timeouts) if timeouts else None
            self.failures: float = float(failures) if failures else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.source and self.server

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[PerfFailuresEntry]):
    return meta.ingest(entries, COLLECTION, filter_fields=["date", "source", "server"])


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=5, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return PerfFailuresEntry
