import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/onionperf-latencies.csv?start={}&end={}"
COLLECTION = "perfroundtrip"


class PerfRoundtrip:
    def __init__(
        self,
        date: str,
        source: str,
        server: str,
        low: str = "",
        q1: str = "",
        md: str = "",
        q3: str = "",
        high: str = "",
    ):
        try:
            self.date = dup.parse(date)
            self.source: str = source
            self.server: str = server
            self.low: int = int(low) if low else None
            self.q1: int = int(q1) if q1 else None
            self.md: int = int(md) if md else None
            self.q3: int = int(q3) if q3 else None
            self.high: int = int(high) if high else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return (
            self.source
            and self.server
            and self.low
            and self.q1
            and self.md
            and self.q3
            and self.high
        )

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[PerfRoundtrip]):
    return meta.ingest(entries, COLLECTION)


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=8, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return PerfRoundtrip
