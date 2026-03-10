import sqlite3

db = "drivehud.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()

print("Tables found:")
for t in tables:
    print("-", t[0])

conn.close()
