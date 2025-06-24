"""Get the end times (time of last finished task) for each taskforce."""

import argparse
from datetime import datetime
from pathlib import Path


def get_final_time_for_taskforce(tf_path: Path) -> datetime | None:
    latest_time: datetime | None = None

    for err_file in tf_path.rglob("*.err"):
        try:
            with err_file.open() as f:
                for line in f:
                    if "just now" not in line:
                        continue
                    timestamp_str = line[:23]  # '2025-06-24 13:04:17.464'
                    try:
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                        if latest_time is None or dt > latest_time:
                            latest_time = dt
                    except ValueError:
                        raise
        except Exception as e:
            print(f"[WARN] Could not read {err_file}: {e}")

    return latest_time


def main():
    parser = argparse.ArgumentParser(
        description="Get end times for one or more taskforces."
    )
    parser.add_argument(
        "dir",
        type=Path,
        help="A taskforce dir or a directory containing multiple 'ewms-taskforce-TF-*' dirs (default: current directory)",
    )
    args = parser.parse_args()

    # Build list of taskforce directories to process
    if args.dir.name.startswith("ewms-taskforce-TF-"):
        tf_dirs = [args.dir]
    else:
        tf_dirs = [p for p in args.dir.glob("ewms-taskforce-TF-*") if p.is_dir()]
    tf_dirs = [p.resolve() for p in tf_dirs]

    results: dict[str, datetime] = {}

    # parse
    for tf_path in tf_dirs:
        final_time = get_final_time_for_taskforce(tf_path)
        results[tf_path.name] = final_time

    # print
    for tf_id in sorted(results):
        print(f"{tf_id} {results[tf_id].isoformat(sep=' ')}")


if __name__ == "__main__":
    main()
