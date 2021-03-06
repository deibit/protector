import mysql.connector

from utils.log import logger
from utils.env import env

mysql_config = {
    "host": "protector_mysql",
    "user": env.get("MYSQL_USER"),
    "password": env.get("MYSQL_PASSWORD"),
    "database": env.get("MYSQL_DATABASE"),
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
    "onionservices": "CREATE TABLE onionservices (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, onions FLOAT, fracs FLOAT);",
    "alerts": "CREATE TABLE alerts (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME, country VARCHAR(2), users INT, trend VARCHAR(4), max INT, min INT, detector VARCHAR(128), sent BOOLEAN);",
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
    "onionservices": "INSERT INTO onionservices (date, onions, fracs) values (%s, %s, %s);",
    "alerts": "INSERT INTO alerts (date, country, users, trend, max, min, detector, sent) VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE);",
}

selects = {
    "users": "SELECT * FROM protector.users WHERE country = %s ORDER BY date DESC LIMIT 180;",
    "alerts": "SELECT * FROM protector.alerts LIMIT 30 ORDER BY date DESC;",
    "newalerts": "SELECT * FROM protector.alerts WHERE sent = FALSE ORDER BY date DESC;",
}

updates = {"marksentalerts": "UPDATE alerts SET sent = TRUE WHERE sent = FALSE;"}


def check_tables() -> bool:
    logger.info("Check needed tables")
    c = connect()
    cursor = c.cursor(dictionary=True)
    table_names = tables.keys()
    for name in table_names:
        cursor.execute(f"SHOW TABLES LIKE '{name}';")
        res = cursor.fetchall()
        if len(res) == 0:
            logger.info(f"Table {name} does not exists. Creating.")
            cursor.execute(tables[name])
    return True


def connect():
    try:
        return mysql.connector.connect(**mysql_config)
    except Exception as e:
        logger.exception(e)
