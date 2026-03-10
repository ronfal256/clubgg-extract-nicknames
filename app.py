import csv
import io
import os
import sqlite3
import tempfile
import xml.etree.ElementTree as ET

from flask import Flask, Response, render_template, request


app = Flask(__name__)


def extract_players_from_db(db_path: str) -> set[tuple[str, str]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    players: set[tuple[str, str]] = set()

    for (xml_text,) in cur.execute(
        "SELECT HandHistory FROM HandHistories WHERE HandHistory IS NOT NULL"
    ):
        if not xml_text:
            continue
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
    return players


@app.route("/", methods=["GET"])
def index() -> str:
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload() -> Response:
    file = request.files.get("dbfile")

    if not file or file.filename == "":
        return Response("No file uploaded.", status=400)

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        file.save(tmp)
        tmp_path = tmp.name

    try:
        players = extract_players_from_db(tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["PlayerName", "PlayerNick"])
    for name, nick in sorted(players):
        writer.writerow([name, nick])

    csv_bytes = output.getvalue().encode("utf-8")

    headers = {
        "Content-Disposition": 'attachment; filename="players.csv"',
    }
    return Response(csv_bytes, mimetype="text/csv", headers=headers)


if __name__ == "__main__":
    app.run(debug=True)

