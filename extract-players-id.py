
import sqlite3
import xml.etree.ElementTree as ET
import csv
from pathlib import Path
import os

db_path = os.environ.get("DB_PATH", "yuval.db")
out_csv = "players.csv"

if not Path(db_path).exists():
    raise FileNotFoundError(
        f"Database file not found: {db_path}. "
        "Place it in the repo or set DB_PATH to its location."
    )

conn = sqlite3.connect(db_path)
cur = conn.cursor()

players = set()

for (xml_text,) in cur.execute(
    "SELECT HandHistory FROM HandHistories WHERE HandHistory IS NOT NULL"
):
    try:
        root = ET.fromstring(xml_text)
        for p in root.findall(".//Players/Player"):
            name = p.attrib.get("PlayerName")
            nick = p.attrib.get("PlayerNick")
            if name and nick:
                players.add((name, nick))
    except ET.ParseError:
        continue

conn.close()

# write CSV
with open(out_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["PlayerName", "PlayerNick"])
    for name, nick in sorted(players):
        writer.writerow([name, nick])

print(f"Saved {len(players)} distinct players to {out_csv}")