import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/torperf.csv?start={}&end={}"
COLLECTION = "perfttd"


class PerfTTDEntry:
    def __init__(
        self,
        date: str,
        filesize: str,
        source: str,
        server: str = 0,
        q1: str = 0,
        md: str = 0,
        q3: str = 0,
    ):
        try:
            self.date = dup.parse(date)
            self.filesize: int = int(filesize) if filesize else None
            self.source: str = source
            self.server: str = server
            self.q1: str = float(q1) if q1 else None
            self.md: str = float(md) if md else None
            self.q3: str = float(q3) if q3 else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return (
            self.filesize
            and self.source
            and self.server
            and self.q1
            and self.md
            and self.q3
        )

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[PerfTTDEntry], init=False):
    return meta.ingest(
        entries, COLLECTION, filter_fields=["date", "filesize", "server"], init=init
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=7, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return PerfTTDEntry
