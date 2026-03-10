
import sqlite3
import xml.etree.ElementTree as ET
import csv
from pathlib import Path
import os
import io
import json
from typing import Optional, Tuple

DEFAULT_DB_PATH = "yuval.db"
DEFAULT_OUT_CSV = "players_id.csv"
DEFAULT_SOURCE_FILENAME = "kiddo.db"


def _get_drive_service():
    """
    Auth options (first match wins):
    - GOOGLE_SERVICE_ACCOUNT_JSON: service account JSON as a string
    - GOOGLE_APPLICATION_CREDENTIALS: path to a service account JSON file
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "Missing Google Drive dependencies. Install: "
            "google-api-python-client google-auth"
        ) from e

    scopes = ["https://www.googleapis.com/auth/drive"]

    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    creds = None
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds_path:
            raise RuntimeError(
                "Google Drive mode requires GOOGLE_SERVICE_ACCOUNT_JSON or "
                "GOOGLE_APPLICATION_CREDENTIALS."
            )
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=scopes
        )

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _drive_find_file_id_by_name(
    service, *, folder_id: str, filename: str
) -> Optional[str]:
    safe_name = filename.replace("'", "\\'")
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{safe_name}' and "
        "trashed = false"
    )
    resp = (
        service.files()
        .list(
            q=q,
            fields="files(id,name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=10,
        )
        .execute()
    )
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def _drive_download_file(service, *, file_id: str, dest_path: str) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _drive_upload_file_to_folder(
    service, *, folder_id: str, local_path: str, dest_name: str
) -> str:
    from googleapiclient.http import MediaFileUpload

    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    file_metadata = {"name": dest_name, "parents": [folder_id]}
    created = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created.get("webViewLink") or created["id"]


def _extract_players_to_csv(*, db_path: str, out_csv: str) -> int:
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

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PlayerName", "PlayerNick"])
        for name, nick in sorted(players):
            writer.writerow([name, nick])

    return len(players)


def main() -> Tuple[int, Optional[str]]:
    """
    Modes:
    - Local (default): reads DB_PATH (or yuval.db), writes OUTPUT_CSV (or players_id.csv)
    - Google Drive: if SOURCE_FILE_ID or SOURCE_FOLDER_ID is provided, downloads kiddo.db,
      runs extraction, uploads CSV to DEST_FOLDER_ID.
    """
    out_csv = os.environ.get("OUTPUT_CSV", DEFAULT_OUT_CSV)

    source_file_id = os.environ.get("SOURCE_FILE_ID")
    source_folder_id = os.environ.get("SOURCE_FOLDER_ID")
    source_filename = os.environ.get("SOURCE_FILENAME", DEFAULT_SOURCE_FILENAME)
    dest_folder_id = os.environ.get("DEST_FOLDER_ID")

    use_drive = bool(source_file_id or source_folder_id or dest_folder_id)

    db_path = os.environ.get("DB_PATH", DEFAULT_DB_PATH)
    uploaded_link = None

    if use_drive:
        if not dest_folder_id:
            raise RuntimeError("DEST_FOLDER_ID is required for Google Drive upload.")
        service = _get_drive_service()

        if not source_file_id:
            if not source_folder_id:
                raise RuntimeError(
                    "Provide SOURCE_FILE_ID or SOURCE_FOLDER_ID for Google Drive download."
                )
            source_file_id = _drive_find_file_id_by_name(
                service, folder_id=source_folder_id, filename=source_filename
            )
            if not source_file_id:
                raise FileNotFoundError(
                    f"Could not find '{source_filename}' in source folder {source_folder_id}."
                )

        db_path = "kiddo.db"
        _drive_download_file(service, file_id=source_file_id, dest_path=db_path)

    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"Database file not found: {db_path}. "
            "Place it in the repo or set DB_PATH to its location."
        )

    count = _extract_players_to_csv(db_path=db_path, out_csv=out_csv)

    if use_drive:
        service = _get_drive_service()
        uploaded_link = _drive_upload_file_to_folder(
            service, folder_id=dest_folder_id, local_path=out_csv, dest_name=out_csv
        )

    return count, uploaded_link

if __name__ == "__main__":
    count, link = main()
    if link:
        print(f"Saved {count} distinct players to {DEFAULT_OUT_CSV} and uploaded to {link}")
    else:
        print(f"Saved {count} distinct players to {DEFAULT_OUT_CSV}")