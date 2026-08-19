
import sqlite3
import xml.etree.ElementTree as ET
import csv
import re
from pathlib import Path
import os
import io
import json
import tempfile
from typing import Iterable, Optional, Tuple, List, Dict, Any

DEFAULT_CLUBGG_OUT_CSV = "players_id_clubgg.csv"
DEFAULT_7XL_OUT_CSV = "players_id_7xl.csv"
DEFAULT_SOURCE_FILENAME = "drivehud.db"
DEFAULT_SOURCE_GLOB_EXT = ".db"
DEFAULT_DRIVE_PLAYERS_CSV = "players_id.csv"

CLUBGG_SITE_ID = 40
SEVEN_XL_SITE_ID = 16


def _get_drive_service():
    """
    Auth options (first match wins):
    - GCP_WORKLOAD_IDENTITY: set to "1" to use Application Default Credentials,
      i.e. the short-lived token minted by google-github-actions/auth's Workload
      Identity Federation step (no stored key involved).
    - GOOGLE_SERVICE_ACCOUNT_JSON: service account JSON as a string
    - GOOGLE_APPLICATION_CREDENTIALS: path to a service account JSON file
    - GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET / GOOGLE_OAUTH_REFRESH_TOKEN:
      user OAuth credentials (personal Google account)
    """
    try:
        import google.auth
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "Missing Google Drive dependencies. Install: "
            "google-api-python-client google-auth"
        ) from e

    scopes = ["https://www.googleapis.com/auth/drive"]

    creds: Optional[object] = None

    # 0) Workload Identity Federation (short-lived, no stored key)
    if os.environ.get("GCP_WORKLOAD_IDENTITY"):
        creds, _ = google.auth.default(scopes=scopes)
    else:
        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

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
                        "GCP_WORKLOAD_IDENTITY, GOOGLE_SERVICE_ACCOUNT_JSON / "
                        "GOOGLE_APPLICATION_CREDENTIALS, or GOOGLE_OAUTH_CLIENT_ID / "
                        "GOOGLE_OAUTH_CLIENT_SECRET / GOOGLE_OAUTH_REFRESH_TOKEN."
                    )

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _drive_list_files_in_folder(service, *, folder_id: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    q = f"'{folder_id}' in parents and trashed = false"

    while True:
        resp = (
            service.files()
            .list(
                q=q,
                fields="nextPageToken,files(id,name,mimeType,size,shortcutDetails)",
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

    # Resolve Drive shortcuts to the file they point at, so downloads target
    # the actual binary content instead of the (non-downloadable) shortcut.
    for f in files:
        shortcut = f.get("shortcutDetails")
        if f.get("mimeType") == "application/vnd.google-apps.shortcut" and shortcut:
            f["id"] = shortcut.get("targetId", f["id"])
            f["mimeType"] = shortcut.get("targetMimeType", f.get("mimeType"))

    return files


def _drive_find_file_id_by_name(
    service, *, folder_id: str, filename: str
) -> Optional[str]:
    safe_name = filename.replace("'", "\\'")
    q = f"'{folder_id}' in parents and name = '{safe_name}' and trashed = false"
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


def _drive_download_file(service, *, file_id: str, dest_path: str) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _extract_players_from_db(*, db_path: str, site_id: Optional[int] = None) -> List[Tuple[str, str]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    players = set()

    query = "SELECT HandHistory FROM HandHistories WHERE HandHistory IS NOT NULL"
    params: list = []
    if site_id is not None:
        query += " AND PokerSiteId = ?"
        params.append(site_id)

    for (xml_text,) in cur.execute(query, params):
        text = xml_text.strip()
        if text.startswith("<"):
            # XML format (ClubGG)
            try:
                root = ET.fromstring(text)
                for p in root.findall(".//Players/Player"):
                    name = p.attrib.get("PlayerName")
                    nick = p.attrib.get("PlayerNick")
                    if name and nick:
                        players.add((name, nick))
            except ET.ParseError:
                continue
        else:
            # Plain text format (7XL/GGNetwork)
            for line in text.splitlines():
                m = re.match(r"^Seat \d+: (.+?) \(\$[\d,.]+ in chips\)", line)
                if m:
                    player_id = m.group(1)
                    if player_id != "Hero":
                        players.add((player_id, player_id))

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


def _sync_clubgg_csv_to_drive(
    service, *, folder_id: str, filename: str, extracted_rows: List[Tuple[str, str]]
) -> None:
    """
    Reads the current players_id.csv from Drive (if it exists) and appends only
    the (PlayerName, PlayerNick) pairs from extracted_rows that aren't already
    present, leaving existing rows and any manually-edited columns untouched.
    """
    import pandas as pd

    existing_file_id = _drive_find_file_id_by_name(service, folder_id=folder_id, filename=filename)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = str(Path(tmpdir) / filename)

        if existing_file_id:
            _drive_download_file(service, file_id=existing_file_id, dest_path=local_path)
            existing_df = pd.read_csv(local_path)
            for col in ["has_alias", "description"]:
                if col not in existing_df.columns:
                    existing_df[col] = ""
            existing_keys = set(zip(existing_df["PlayerName"], existing_df["PlayerNick"]))
        else:
            existing_df = pd.DataFrame(columns=["PlayerName", "PlayerNick", "has_alias", "description"])
            existing_keys = set()

        rows_to_add = [r for r in sorted(set(extracted_rows)) if r not in existing_keys]
        if not rows_to_add and existing_file_id:
            return

        add_df = pd.DataFrame(rows_to_add, columns=["PlayerName", "PlayerNick"])
        add_df["has_alias"] = ""
        add_df["description"] = ""

        combined_df = pd.concat([existing_df, add_df], ignore_index=True)
        combined_df.to_csv(local_path, index=False, encoding="utf-8")

        _drive_upload_file_to_folder_overwrite(
            service, folder_id=folder_id, local_path=local_path, dest_name=filename
        )


def main() -> Tuple[int, int]:
    """
    Downloads .db files from SOURCE_FOLDER_ID (or SOURCE_FILE_ID) on Google Drive,
    extracts players per site, and writes two local CSVs:
    - players_id_clubgg.csv  (PokerSiteId=40)
    - players_id_7xl.csv     (PokerSiteId=16)
    Existing local CSVs are merged to preserve has_alias/description metadata.
    """
    clubgg_csv = os.environ.get("CLUBGG_OUTPUT_CSV", DEFAULT_CLUBGG_OUT_CSV)
    seven_xl_csv = os.environ.get("7XL_OUTPUT_CSV", DEFAULT_7XL_OUT_CSV)

    source_file_id = os.environ.get("SOURCE_FILE_ID")
    source_folder_id = os.environ.get("SOURCE_FOLDER_ID")
    source_filename = os.environ.get("SOURCE_FILENAME", DEFAULT_SOURCE_FILENAME)
    source_ext = os.environ.get("SOURCE_EXT", DEFAULT_SOURCE_GLOB_EXT)

    service = _get_drive_service()

    file_ids: List[Tuple[str, str]] = []
    if source_file_id:
        file_ids.append((source_file_id, source_filename))
    else:
        if not source_folder_id:
            raise RuntimeError("Provide SOURCE_FOLDER_ID or SOURCE_FILE_ID.")
        files = _drive_list_files_in_folder(service, folder_id=source_folder_id)
        db_files = [
            f
            for f in files
            if isinstance(f.get("name"), str)
            and f["name"].lower().endswith(source_ext.lower())
        ]
        if not db_files:
            raise FileNotFoundError(
                f"No files ending with '{source_ext}' found in source folder {source_folder_id}."
            )
        file_ids.extend((f["id"], f["name"]) for f in db_files)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_paths = []
        for file_id, name in file_ids:
            local_path = str(Path(tmpdir) / name)
            _drive_download_file(service, file_id=file_id, dest_path=local_path)
            db_paths.append(local_path)

        clubgg_rows: List[Tuple[str, str]] = []
        seven_xl_rows: List[Tuple[str, str]] = []
        for db_path in db_paths:
            clubgg_rows.extend(_extract_players_from_db(db_path=db_path, site_id=CLUBGG_SITE_ID))
            seven_xl_rows.extend(_extract_players_from_db(db_path=db_path, site_id=SEVEN_XL_SITE_ID))

    clubgg_count = _write_players_dataframe(
        rows=clubgg_rows,
        out_csv=clubgg_csv,
        old_csv=clubgg_csv if Path(clubgg_csv).exists() else None,
    )
    seven_xl_count = _write_players_dataframe(
        rows=seven_xl_rows,
        out_csv=seven_xl_csv,
        old_csv=seven_xl_csv if Path(seven_xl_csv).exists() else None,
    )

    if source_folder_id:
        drive_players_csv = os.environ.get("DRIVE_PLAYERS_CSV", DEFAULT_DRIVE_PLAYERS_CSV)
        _sync_clubgg_csv_to_drive(
            service,
            folder_id=source_folder_id,
            filename=drive_players_csv,
            extracted_rows=clubgg_rows,
        )

    return clubgg_count, seven_xl_count


if __name__ == "__main__":
    clubgg_count, seven_xl_count = main()
    print(f"ClubGG: {clubgg_count} distinct players -> {DEFAULT_CLUBGG_OUT_CSV}")
    print(f"7XL:    {seven_xl_count} distinct players -> {DEFAULT_7XL_OUT_CSV}")
