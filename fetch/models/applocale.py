import dateutil.parser as dup

from log import logger

from fetch.models import meta

URL = "https://metrics.torproject.org/webstats-tb-locale.csv?start={}&end={}"
COLLECTION = "applocale"


class AppLocaleEntry:
    def __init__(
        self,
        date: str,
        locale: str,
        initial_downloads: str = "",
        update_pings: str = "",
    ):
        try:
            self.date = dup.parse(date)
            self.locale = locale.upper()
            self.initial_downloads = int(initial_downloads)
            self.update_pings = int(update_pings) if update_pings else None

        except Exception as e:
            logger.exception(e)

    def validate(self) -> bool:
        return self.locale and self.initial_downloads

    def serialize(self) -> dict:
        return self.__dict__


def ingest(entries: list[AppLocaleEntry], init=False):
    return meta.ingest(entries, COLLECTION, filter_fields=["locale", "date"], init=init)


def purify(entries: list[str]) -> list[list[str]]:
    return meta.purify(entries=entries, fields=4, banned=["date", "#"])


def last():
    return meta.last(COLLECTION)


def get_class():
    return AppLocaleEntry
