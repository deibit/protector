from log import logger

import db

MAX_SIZE = 10000
CHUNKS = 10


def ingest(entries: list, table_name: str) -> None:
    try:
        validated = [e.serialize() for e in entries if e.validate()]
        length = len(validated)
        inserted = 0

        con = db.connect()
        cur = con.cursor()

        if length > MAX_SIZE:
            logger.info("Due to ingest size commits will be batched")
            chunk_size = round(length / CHUNKS)

            for index in range(0, CHUNKS):
                if index == CHUNKS - 1:
                    shift = None
                else:
                    shift = index * chunk_size + chunk_size

                chunk = validated[index * chunk_size : shift]

                cur.executemany(
                    db.insertions[table_name],
                    chunk,
                )

                inserted += cur.rowcount
                con.commit()
        else:
            cur.executemany(db.insertions[table_name], validated)
            inserted += cur.rowcount
            con.commit()

        logger.info(
            "Ingested %s/%s entries for %s of %s valid rows",
            inserted,
            len(entries),
            table_name,
            length,
        )

    except Exception as e:
        logger.exception(e)


def last(table: str):
    c = db.connect()
    cur = c.cursor(dictionary=True)
    cur.execute(f"select date from {table} where id=(SELECT MAX(id) from {table});")
    return cur.fetchone()


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
