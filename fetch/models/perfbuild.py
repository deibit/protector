import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/onionperf-buildtimes.csv?start={}&end={}"
COLLECTION = "perfbuild"


class PerfBuild:
    def __init__(
        self,
        date: str,
        source: str,
        position: str,
        q1: str = "",
        md: str = "",
        q3: str = "",
    ):
        try:
            self.date = dup.parse(date)
            self.source: str = source
            self.position: str = position
            self.q1: float = float(q1) if q1 else None
            self.md: float = float(md) if md else None
            self.q3: float = float(q3) if q3 else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.source and self.position and self.q1 and self.md and self.q3

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[PerfBuild]):
    return meta.ingest(
        entries, COLLECTION, filter_fields=["date", "source", "position"]
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=6, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return PerfBuild
