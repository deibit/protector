from utils.db import connect

c = connect()

SQL = "SELECT * FROM onionservices;"

data = c.cursor().execute(SQL)

for d in data:
    print(d)
