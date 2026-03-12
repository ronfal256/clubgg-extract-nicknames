
import sqlite3
import xml.etree.ElementTree as ET
import csv
from pathlib import Path
import os
import io
import json
import tempfile
from typing import Iterable, Optional, Tuple, List, Dict, Any

DEFAULT_OUT_CSV = "players_id.csv"
DEFAULT_SOURCE_FILENAME = "drivehud.db"
DEFAULT_SOURCE_GLOB_EXT = ".db"


def _get_drive_service():
    """
    Auth options (first match wins):
    - GOOGLE_SERVICE_ACCOUNT_JSON: service account JSON as a string
    - GOOGLE_APPLICATION_CREDENTIALS: path to a service account JSON file
    - GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET / GOOGLE_OAUTH_REFRESH_TOKEN:
      user OAuth credentials (personal Google account)
    """
    try:
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "Missing Google Drive dependencies. Install: "
            "google-api-python-client google-auth"
        ) from e

    scopes = ["https://www.googleapis.com/auth/drive"]

    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    creds: Optional[object] = None

    # 1) Service account via inline JSON
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        # 2) Service account via JSON file path
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path:
            creds = service_account.Credentials.from_service_account_file(
                creds_path, scopes=scopes
            )
        else:
            # 3) User OAuth via refresh token
            client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
            client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
            refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN")
            if client_id and client_secret and refresh_token:
                creds = Credentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=scopes,
                )
            else:
                raise RuntimeError(
                    "Google Drive auth is not configured. Set either "
                    "GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_APPLICATION_CREDENTIALS "
                    "or GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET / "
                    "GOOGLE_OAUTH_REFRESH_TOKEN."
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


def _drive_list_files_in_folder(service, *, folder_id: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    q = f"'{folder_id}' in parents and trashed = false"

    while True:
        resp = (
            service.files()
            .list(
                q=q,
                fields="nextPageToken,files(id,name,mimeType,size)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
                pageSize=1000,
            )
            .execute()
        )
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


def _drive_download_file(service, *, file_id: str, dest_path: str) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _drive_upload_file_to_folder_overwrite(
    service, *, folder_id: str, local_path: str, dest_name: str
) -> str:
    from googleapiclient.http import MediaFileUpload

    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    existing_id = _drive_find_file_id_by_name(service, folder_id=folder_id, filename=dest_name)
    if existing_id:
        updated = (
            service.files()
            .update(
                fileId=existing_id,
                media_body=media,
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        return updated.get("webViewLink") or updated["id"]
    else:
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


def _extract_players_from_db(*, db_path: str) -> List[Tuple[str, str]]:
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

    return sorted(players)


def _write_players_dataframe(*, rows, out_csv: str, old_csv: Optional[str] = None) -> int:
    import pandas as pd

    new_df = pd.DataFrame(rows, columns=["PlayerName", "PlayerNick"]).drop_duplicates()

    if old_csv and Path(old_csv).exists():
        old_df = pd.read_csv(old_csv)

        for col in ["has_alias", "description"]:
            if col not in old_df.columns:
                old_df[col] = ""

        df = new_df.merge(
            old_df[["PlayerName", "PlayerNick", "has_alias", "description"]],
            on=["PlayerName", "PlayerNick"],
            how="left"
        )
    else:
        new_df["has_alias"] = ""
        new_df["description"] = ""
        df = new_df

    df.to_csv(out_csv, index=False, encoding="utf-8")
    return len(df)

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
    source_ext = os.environ.get("SOURCE_EXT", DEFAULT_SOURCE_GLOB_EXT)
    dest_folder_id = os.environ.get("DEST_FOLDER_ID")

    use_drive = bool(source_file_id or source_folder_id or dest_folder_id)

    db_path = os.environ.get("DB_PATH")
    uploaded_link = None

    all_rows: List[Tuple[str, str]] = []

    if use_drive:
        if not dest_folder_id:
            raise RuntimeError("DEST_FOLDER_ID is required for Google Drive upload.")
        service = _get_drive_service()

        file_ids: List[Tuple[str, str]] = []

        if source_file_id:
            file_ids.append((source_file_id, source_filename))
        else:
            if not source_folder_id:
                raise RuntimeError("Provide SOURCE_FOLDER_ID to download all .hud files.")

            files = _drive_list_files_in_folder(service, folder_id=source_folder_id)
            hud_files = [
                f
                for f in files
                if isinstance(f.get("name"), str)
                and f["name"].lower().endswith(source_ext.lower())
            ]
            if not hud_files:
                raise FileNotFoundError(
                    f"No files ending with '{source_ext}' found in source folder {source_folder_id}."
                )
            file_ids.extend((f["id"], f["name"]) for f in hud_files)

        with tempfile.TemporaryDirectory() as tmpdir:
            for file_id, name in file_ids:
                local_path = str(Path(tmpdir) / name)
                _drive_download_file(service, file_id=file_id, dest_path=local_path)
                all_rows.extend(_extract_players_from_db(db_path=local_path))
    else:
        # Local mode: process a single DB file
        if not Path(db_path).exists():
            raise FileNotFoundError(
                f"Database file not found: {db_path}. "
                "Place it in the repo or set DB_PATH to its location."
            )
        all_rows.extend(_extract_players_from_db(db_path=db_path))

    # Always overwrite output file
    old_csv_path = None

    if use_drive:
        service = _get_drive_service()

        # try to find existing CSV in Drive
        existing_file_id = _drive_find_file_id_by_name(
            service,
            folder_id=dest_folder_id,
            filename=out_csv
        )

        if existing_file_id:
            old_csv_path = "previous_players_id.csv"
            _drive_download_file(
                service,
                file_id=existing_file_id,
                dest_path=old_csv_path
            )

    count = _write_players_dataframe(
        rows=all_rows,
        out_csv=out_csv,
        old_csv=old_csv_path
    )

    if use_drive:
        service = _get_drive_service()
        uploaded_link = _drive_upload_file_to_folder_overwrite(
            service, folder_id=dest_folder_id, local_path=out_csv, dest_name=out_csv
        )

    return count, uploaded_link

if __name__ == "__main__":
    count, link = main()
    if link:
        print(f"Saved {count} distinct players to {DEFAULT_OUT_CSV} and uploaded to {link}")
    else:
        print(f"Saved {count} distinct players to {DEFAULT_OUT_CSV}")