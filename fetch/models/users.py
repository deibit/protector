import dateutil.parser as dup

from utils.log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/userstats-relay-country.csv?start={}&end={}"
COLLECTION = "users"


def _str2number(s) -> int:
    try:
        result = int(s)
    except ValueError:
        try:
            result = int(float(s))
        except Exception:
            result = None
    finally:
        return result


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
            self.upper = _str2number(upper)
            self.frac = int(frac) if frac else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.country and self.users and self.lower and self.upper

    def serialize(self) -> tuple:
        return tuple(self.__dict__.values())


def ingest(entries: list[UsersEntry]):
    return meta.ingest(
        entries=entries,
        table_name=COLLECTION,
    )


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=6, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return UsersEntry
