import sqlite3
import sys
from pathlib import Path

SRC = Path(r"C:\Users\ronfa\AppData\Roaming\DriveHUD 2\drivehud.db")
DST = Path(r"G:\My Drive\db\drivehud.db")


def main():
    if not SRC.exists():
        print(f"ERROR: source not found: {SRC}")
        sys.exit(1)

    print(f"Snapshotting {SRC} -> {DST} ...")
    src_conn = sqlite3.connect(str(SRC))
    dst_conn = sqlite3.connect(str(DST))
    src_conn.backup(dst_conn)
    dst_conn.close()
    src_conn.close()
    print("Done. Google Drive will sync the snapshot automatically.")


if __name__ == "__main__":
    main()
