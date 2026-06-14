from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Common.db.db_execute import execute_many


UPSERT_SIGNAL_SQL = """
INSERT INTO signal_table_15min (
    uuid, asset_1, asset_2, tp, sl, rolling_window_15m,
    open_threshold_multiplier, entry_z_threshold,
    zscore_sl_threshold, enabled, last_update_ts
)
VALUES (
    %(uuid)s, %(asset_1)s, %(asset_2)s, %(tp)s, %(sl)s,
    %(rolling_window_15m)s, %(open_threshold_multiplier)s,
    %(entry_z_threshold)s, %(zscore_sl_threshold)s,
    %(enabled)s, %(last_update_ts)s
)
ON DUPLICATE KEY UPDATE
    asset_1 = VALUES(asset_1),
    asset_2 = VALUES(asset_2),
    tp = VALUES(tp),
    sl = VALUES(sl),
    rolling_window_15m = VALUES(rolling_window_15m),
    open_threshold_multiplier = VALUES(open_threshold_multiplier),
    entry_z_threshold = VALUES(entry_z_threshold),
    zscore_sl_threshold = VALUES(zscore_sl_threshold),
    enabled = VALUES(enabled);
"""

UPSERT_STATE_SQL = """
INSERT INTO pair_state_15min (uuid, hl_timeframe_minutes, last_update_ts)
VALUES (%(uuid)s, 60, %(last_update_ts)s)
ON DUPLICATE KEY UPDATE uuid = VALUES(uuid);
"""


def read_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            multiplier = float(raw["open_threshold_multiplier"])
            entry_threshold_raw = str(raw.get("entry_z_threshold") or "").strip()
            entry_threshold = (
                float(entry_threshold_raw)
                if entry_threshold_raw
                else 2.0 * multiplier
            )
            row = {
                "uuid": raw["uuid"].strip(),
                "asset_1": raw["asset_1"].strip(),
                "asset_2": raw["asset_2"].strip(),
                "tp": float(raw["tp"]),
                "sl": float(raw["sl"]),
                "rolling_window_15m": int(raw["rolling_window_15m"]),
                "open_threshold_multiplier": multiplier,
                "entry_z_threshold": entry_threshold,
                "zscore_sl_threshold": float(
                    raw.get("zscore_sl_threshold") or 6
                ),
                "enabled": int(raw.get("enabled") or 1),
                "last_update_ts": datetime.now(timezone.utc).replace(tzinfo=None),
            }
            if row["rolling_window_15m"] not in {100, 200, 300}:
                raise ValueError(
                    f"Unsupported rolling window for {row['uuid']}: "
                    f"{row['rolling_window_15m']}"
                )
            rows.append(row)
    if not rows:
        raise ValueError(f"No pair rows found in {path}")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--mysql-api-file", default="api_mysql_main.txt"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write validated rows to MySQL. Without this flag, only validate.",
    )
    args = parser.parse_args()
    rows = read_rows(args.csv_path)
    print(f"Validated {len(rows)} pair settings from {args.csv_path}")
    if not args.apply:
        print("Dry run only. Add --apply to write database rows.")
        return
    execute_many(UPSERT_SIGNAL_SQL, args.mysql_api_file, rows)
    execute_many(UPSERT_STATE_SQL, args.mysql_api_file, rows)
    print("15-minute signal and pair-state rows imported.")


if __name__ == "__main__":
    main()
