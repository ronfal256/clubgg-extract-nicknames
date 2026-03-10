import sqlite3
import xml.etree.ElementTree as ET
import csv
from collections import defaultdict

DB_PATH = "drivehud.db"
OUTPUT_CSV = "top_bottom_players.csv"

stats = defaultdict(lambda: {"hands": 0, "net": 0.0})

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT HandHistory FROM HandHistories WHERE HandHistory IS NOT NULL")

# keep track of which player/hand combinations we have already counted
counted_hands = set()  # set of (HandId, PlayerName)

for (xml_text,) in cursor.fetchall():
    try:
        root = ET.fromstring(xml_text)
        hand_id = root.findtext("HandId")
        if not hand_id:
            continue

        players = root.find("Players")
        actions = root.find("Actions")
        if players is None or actions is None:
            continue

        for p in players.findall("Player"):
            player_id = p.attrib.get("PlayerName")
            nick = p.attrib.get("PlayerNick")

            # skip if already counted this hand for this player
            if (hand_id, player_id) in counted_hands:
                continue
            counted_hands.add((hand_id, player_id))

            win = float(p.attrib.get("Win", 0))

            # total bet for this player
            total_bet = sum(
                -float(a.attrib.get("Amount", 0))
                for a in actions.findall(f"HandAction[@PlayerName='{player_id}']")
                if a.attrib.get("HandActionType") in (
                    "SMALL_BLIND", "BIG_BLIND", "CALL", "RAISE", "BET", "ANTE"
                )
            )

            # uncalled bets returned to player
            uncalled = sum(
                float(a.attrib.get("Amount", 0))
                for a in actions.findall(f"HandAction[@PlayerName='{player_id}']")
                if a.attrib.get("HandActionType") == "UNCALLED_BET"
            )

            net = win + uncalled - total_bet

            stats[nick]["hands"] += 1
            stats[nick]["net"] += net

    except ET.ParseError:
        continue

conn.close()

# sort by net
sorted_players = sorted(stats.items(), key=lambda x: x[1]["net"], reverse=True)

top_10 = sorted_players[:10]
bottom_10 = sorted_players[-10:]

rows = []
for nick, data in top_10 + bottom_10:
    rows.append([
        nick,
        data["hands"],
        round(data["net"], 2)
    ])

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["player_nick", "hands_played", "total_net_won"])
    writer.writerows(rows)

print(f"✅ Exported {len(rows)} rows to {OUTPUT_CSV}")
