import bz2
import json
import sqlite3
import sys

import requests

from log import logger

URL = "https://collector.torproject.org/index/index.json.bz2"


def download_index():
    try:
        resp = requests.get(URL)

        if not resp.status_code == 200:
            logger.error("Error %s when downloading index", resp.status_code)
            sys.exit(-1)

        return resp.content

    except Exception as e:
        logger.exception("Exception when trying to downloading index")
        logger.exception(e)


def decompress2json(compressed_buffer):
    try:
        return json.loads(bz2.decompress(compressed_buffer))

    except Exception as e:
        logger.exception("Exception when trying to decompress index")
        logger.exception(e)


def _pretty_print(obj):
    # obj = json.dumps(obj, sort_keys=True, indent=4)
    print(obj.get("index_created"))
    print(obj.get("build_revision"))
    print(obj.get("directories")[0].get("path"))


if __name__ == "__main__":
    obj = decompress2json(download_index())
    _pretty_print(obj)
