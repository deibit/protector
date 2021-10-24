import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/hidserv-rend-relayed-cells.csv?start={}&end={}"
COLLECTION = "onionservices"


class OnionServicesEntry:
    def __init__(
        self,
        date: str,
        onions: str,
        fracs: str,
    ):
        try:
            self.date = dup.parse(date)
            self.onions = float(onions) if onions else None
            self.fracs = float(fracs) if fracs else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.onions or self.fracs

    def serialize(self) -> tuple:
        return tuple(self.__dict__.values())


def ingest(entries: list[OnionServicesEntry]):
    return meta.ingest(entries, COLLECTION)


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=3, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return OnionServicesEntry
