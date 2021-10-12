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
        timeouts: str = 0,
        failures: str = 0,
    ):
        try:
            self.date = dup.parse(date)
            self.source: str = source
            self.server: str = server
            self.timeouts: str = float(timeouts) if timeouts else None
            self.failures: str = float(failures) if failures else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.source and self.server

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[PerfFailuresEntry], init=False):
    return meta.ingest(
        entries, COLLECTION, filter_fields=["date", "source", "server"], init=init
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=5, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return PerfFailuresEntry
