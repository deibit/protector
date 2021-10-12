import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/userstats-bridge-country.csv?start={}&end={}"
COLLECTION = "bridges"


class BridgesEntry:
    def __init__(
        self,
        date: str,
        country: str,
        users: str,
        frac: str = 0,
    ):
        try:
            self.date = dup.parse(date)
            self.country = country.upper()
            self.users = int(users)
            self.frac = int(frac) if frac else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.country and self.users

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[BridgesEntry], init=False):
    return meta.ingest(
        entries, COLLECTION, filter_fields=["country", "date"], init=init
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=4, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return BridgesEntry
