import dateutil.parser as dup

from log import logger
from db import connect

import pymongo
from pymongo.collection import Collection


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
            self.users = users
            self.lower = int(lower) if lower else None
            self.upper = int(upper) if upper else None
            self.frac = int(frac) if frac else None
        except Exception as e:
            print(date)
            logger.exception(e)

    def validate(self) -> bool:
        return self.country and self.lower and self.upper

    def serialize(self) -> dict:
        return {
            "date": self.date,
            "country": self.country,
            "lower": self.lower,
            "upper": self.upper,
            "frac": self.frac,
        }


def ingest(users: list[UsersEntry], init=False) -> None:
    try:
        validated = [user for user in users if user.validate()]
        collection: Collection = connect().users

        if init:
            collection.insert_many([user.serialize() for user in validated])

        else:
            for entry in validated:
                entry = entry.serialize()
                collection.update_one(
                    {"country": entry["country"], "date": entry["date"]},
                    {"$set": entry},
                    upsert=True,
                )
        logger.info("Ingested %s/%s users", len(validated), len(users))

    except Exception as e:
        logger.exception(e)


def purify(entries: list[str]) -> list[list[str]]:
    tmp = []

    for entry in entries:

        # Skip comment lines
        if entry.startswith("#"):
            continue

        # Split str into list of params
        entry = entry.split(",")

        # Skip entries with less than ',' params
        if not len(entry) == 6:
            continue

        # Skip head
        if entry[0] == "date":
            continue

        tmp.append(entry)

    return tmp


def last():
    collection: Collection = connect().users
    return collection.find_one({}, limit=1, sort=[("_id", pymongo.DESCENDING)])
