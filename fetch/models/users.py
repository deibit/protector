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
        lower: str = "",
        upper: str = "",
        frac: str = "",
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
        return self.country and self.users and self.lower and self.upper

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[UsersEntry]):
    return meta.ingest(
        entries=entries,
        collection_name=COLLECTION,
        filter_fields=["country", "date"],
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=6, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return UsersEntry
