import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/userstats-relay-country.csv?start={}&end={}"
COLLECTION = "users"


class UsersEntry:
    def __init__(
        self,
        date: str,
        country: str,
        users: str,
        lower: str = 0,
        upper: str = 0,
        frac: str = 0,
    ):
        try:
            self.date = dup.parse(date)
            self.country = country.upper()
            self.users = int(users)
            self.lower = int(lower) if lower else None
            self.upper = int(upper) if upper else None
            self.frac = int(frac) if frac else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.country and self.lower and self.upper

    def serialize(self) -> dict:
        self.__dict__


def ingest(entries: list[UsersEntry], init=False):
    return meta.ingest(
        entries, COLLECTION, filter_fields=["country", "date"], init=init
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=6, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return UsersEntry
