import pymongo
from pymongo.collection import Collection

from log import logger
from db import connect


def ingest(entries: list, collection_name: str, filter_fields: list[str]) -> None:
    try:
        validated = [e for e in entries if e.validate()]
        collection: Collection = connect()[collection_name]
        collection.insert_many([e.serialize() for e in validated])

        logger.info(
            "Ingested %s/%s entries for %s",
            len(validated),
            len(entries),
            collection_name,
        )

    except Exception as e:
        logger.exception(e)


def last(collection_name: str):
    collection: Collection = connect()[collection_name]
    last = collection.find_one({}, limit=1, sort=[("_id", pymongo.DESCENDING)])
    if last:
        logger.info("Last entry on %s was %s", collection_name, last["date"])
    return last


def purify(entries: list[str], fields: int, banned: list[str]) -> list[list[str]]:
    tmp = []

    for entry in entries:

        # Skip comment lines
        if any([entry.startswith(ban_word) for ban_word in banned]):
            continue

        # Split str into list of params
        entry = entry.split(",")

        # Skip entries with less than ',' params
        if not len(entry) == fields:
            continue

        tmp.append(entry)

    return tmp
