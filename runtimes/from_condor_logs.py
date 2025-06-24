"""Get the end times (time of last finished task) for each taskforce."""

from datetime import datetime
from pathlib import Path

BASE_DIR = Path.cwd()
latest_times: dict[str, datetime] = {}

for tf_path in BASE_DIR.glob("ewms-taskforce-TF-*"):
    if not tf_path.is_dir():
        continue

    tf_id = tf_path.name
    latest_time = None

    for err_file in tf_path.rglob("*.err"):
        try:
            with err_file.open() as f:
                for line in f:
                    if "just now" not in line:
                        continue
                    if len(line) < 23:
                        continue  # not enough chars for timestamp
                    timestamp_str = line[:23]  # '2025-06-24 13:04:17.464'
                    try:
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                        if latest_time is None or dt > latest_time:
                            latest_time = dt
                    except ValueError:
                        continue  # malformed timestamp
        except Exception as e:
            print(f"[WARN] Could not read {err_file}: {e}")

    if latest_time:
        latest_times[tf_id] = latest_time

# Print sorted by taskforce ID
for tf_id in sorted(latest_times):
    print(f"{tf_id} {latest_times[tf_id].isoformat(sep=' ')}")
