import bz2
import logging
import json
import sqlite3
import sys

import requests


URL = "https://collector.torproject.org/index/index.json.bz2"


def download_index():
    try:
        resp = requests.get(URL)

        if not resp.status_code == 200:
            logging.error("Error %s when downloading index", resp.status_code)
            sys.exit(-1)

        return resp.content

    except Exception as e:
        logging.exception("Exception when trying to downloading index")
        logging.exception(e)


def decompress2json(compressed_buffer):
    return json.loads(bz2.decompress(compressed_buffer))


def _pretty_print(obj):
    print(json.dumps(obj, sort_keys=True, indent=4))


if __name__ == "__main__":
    obj = decompress2json(download_index())
    _pretty_print(obj)
