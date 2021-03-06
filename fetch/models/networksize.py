import dateutil.parser as dup

from utils.log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/networksize.csv?start={}&end={}"
COLLECTION = "networksize"


class NetworkSizeEntry:
    def __init__(
        self,
        date: str,
        relays: str,
        bridges: str,
    ):
        try:
            self.date = dup.parse(date)
            self.relays = int(relays) if relays else None
            self.bridges = int(bridges) if bridges else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.relays and self.bridges

    def serialize(self) -> tuple:
        return tuple(self.__dict__.values())


def ingest(entries: list[NetworkSizeEntry]):
    return meta.ingest(entries, COLLECTION)


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=3, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return NetworkSizeEntry
