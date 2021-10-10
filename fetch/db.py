import os

import pymongo

from log import logger

mongo_config = {
    "db": os.environ["MONGO_INITDB_DATABASE"],
    "username": os.environ["MONGO_INITDB_ROOT_USERNAME"],
    "password": os.environ["MONGO_INITDB_ROOT_PASSWORD"],
    "host": os.environ["MONGO_HOST"],
    "port": os.environ["MONGO_PORT"],
    "auth_src": "admin",
}


def connect() -> pymongo.database.Database:
    try:
        return pymongo.MongoClient(
            "mongodb://{username}:{password}@{host}:{port}/{db}?authSource={auth_src}".format(
                **mongo_config
            )
        ).protector
    except Exception as e:
        logger.exception(e)
