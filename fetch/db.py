import os

import pymongo

import mysql.connector

from log import logger

mongo_config = {
    "db": os.environ["MONGO_INITDB_DATABASE"],
    "username": os.environ["MONGO_INITDB_ROOT_USERNAME"],
    "password": os.environ["MONGO_INITDB_ROOT_PASSWORD"],
    "host": os.environ["MONGO_HOST"],
    "port": os.environ["MONGO_PORT"],
    "auth_src": "admin",
}

mysql_config = {
    "host": "127.0.0.1",
    "user": os.environ["MYSQL_USER"],
    "password": os.environ["MYSQL_PASSWORD"],
    "database": os.environ["MYSQL_DATABASE"],
}


tables = {
    "applocale": "CREATE TABLE applocale (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, locale VARCHAR(5), initial_downloads INT, update_pings INT);",
    "bandwidth": "CREATE TABLE bandwidth (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, advbw FLOAT, bwhist FLOAT);",
    "bridges": "CREATE TABLE bridges (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, country VARCHAR(2), users INT, frac INT);",
    "networksize": "CREATE TABLE networksize (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, relays INT, bridges INT);",
    "perfbuild": "CREATE TABLE perfbuild (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, source VARCHAR(100), position VARCHAR(100), q1 FLOAT, md FLOAT, q3 FLOAT);",
    "perffailures": "CREATE TABLE perffailures (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, source VARCHAR(100), server VARCHAR(255), timeouts FLOAT, failures FLOAT);",
    "perfroundtrip": "CREATE TABLE perfroundtrip (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, source VARCHAR(100), server VARCHAR(255), low INT, q1 INT, md INT, q3 INT, high INT);",
    "perfthroughput": "CREATE TABLE perfthroughput (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, source VARCHAR(100), server VARCHAR(255), low INT, q1 INT, md INT, q3 INT, high INT);",
    "perfttd": "CREATE TABLE perfttd (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, filesize INT, source VARCHAR(100), server VARCHAR(255), q1 FLOAT, md FLOAT, q3 FLOAT);",
    "users": "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, country VARCHAR(2), users INT, lower INT, upper INT, frac INT);",
}

insertions = {
    "applocale": "INSERT INTO applocale (date, locale, initial_downloads, update_pings) values (%s, %s, %s, %s);",
    "bandwidth": "INSERT INTO bandwidth (date, advbw, bwhist) values (%s, %s, %s);",
    "bridges": "INSERT INTO bridges (date, country, users, frac) values (%s, %s, %s, %s);",
    "networksize": "INSERT INTO networksize (date, relays, bridges) values (%s, %s, %s);",
    "perfbuild": "INSERT INTO perfbuild (date, source, position, q1, md, q3) values (%s, %s, %s, %s, %s, %s);",
    "perffailures": "INSERT INTO perffailures (date, source, server, timeouts, failures) values (%s, %s, %s, %s, %s);",
    "perfroundtrip": "INSERT INTO perfroundtrip (date, source, server, low, q1, md, q3, high) values (%s, %s, %s, %s, %s, %s, %s, %s);",
    "perfthroughput": "INSERT INTO perfthroughput (date, source, server, low, q1, md, q3, high) values (%s, %s, %s, %s, %s, %s, %s, %s);",
    "perfttd": "INSERT INTO perfttd (date, filesize, source, server, q1, md, q3) values (%s, %s, %s, %s, %s, %s, %s);",
    "users": "INSERT INTO users (date, country, users, lower, upper, frac) values (%s, %s, %s, %s, %s, %s);",
}


def _first_run() -> bool:
    c = connect()
    cursor = c.cursor(dictionary=True)
    cursor.execute("SHOW TABLES LIKE 'users';")
    res = cursor.fetchall()
    if len(res) > 0:
        return False
    return True


def _populate():
    con = connect()
    c = con.cursor(dictionary=True)
    for _, v in tables.items():
        try:
            c.execute(v)
        except Exception:
            # In case table exists just skip the exception
            continue


def mongo():
    return pymongo.MongoClient(
        "mongodb://{username}:{password}@{host}:{port}/{db}?authSource={auth_src}".format(
            **mongo_config
        )
    ).protector


def connect():
    try:
        return mysql.connector.connect(**mysql_config)
    except Exception as e:
        logger.exception(e)


logger.info("Checking for database initialization")
if _first_run():
    logger.info("Creating all needed tables")
    _populate()
logger.info("Tables should be already created")
