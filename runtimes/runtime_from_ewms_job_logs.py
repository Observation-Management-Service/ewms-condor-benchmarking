"""Get the end times (time of last finished task) for each taskforce."""

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_final_time_for_taskforce(tf_path: Path) -> datetime | None:
    latest_time: datetime | None = None

    print(f"parsing {tf_path}...")

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

    print(f"-> {latest_time}")
    return latest_time


async def get_creation_time_for_wf(rc: RestClient, tf_dname: str) -> datetime:

    def _to_workflow_id(tf_dir: str) -> str:
        # ewms-taskforce-TF-685643b1-7a50f872-ba2017b8-05bfe8b1 -> WF-685643b1-7a50f872
        parts = tf_dir.split("-")
        return f"WF-{parts[2]}-{parts[3]}"

    workflow_id = _to_workflow_id(tf_dname)

    resp = await rc.request("GET", f"/v1/workflows/{workflow_id}")

    return datetime.fromtimestamp(resp["timestamp"])


async def main():
    parser = argparse.ArgumentParser(
        description="Get end times for one or more taskforces."
    )
    parser.add_argument(
        "dir",
        type=Path,
        help="A taskforce dir or a directory containing multiple 'ewms-taskforce-TF-*' dirs (default: current directory)",
    )
    args = parser.parse_args()

    rc = SavedDeviceGrantAuth(
        "https://ewms-dev.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename=str(Path("~/ewms-dev-device-refresh-token").expanduser().resolve()),
        client_id="ewms-dev-public",
        retries=0,
    )

    # Build list of taskforce directories to process
    if args.dir.name.startswith("ewms-taskforce-TF-"):
        tf_dirs = [args.dir]
    else:
        tf_dirs = [p for p in args.dir.glob("ewms-taskforce-TF-*") if p.is_dir()]
    tf_dirs = [p.resolve() for p in tf_dirs]
    print(f"looking at {[str(d) for d in tf_dirs]}...")

    # get endtimes
    endtimes: dict[str, datetime] = {}
    for tf_path in tf_dirs:
        final_time = get_final_time_for_taskforce(tf_path)
        endtimes[tf_path.name] = final_time
    for tf_id in sorted(endtimes):
        print(f"{tf_id} {endtimes[tf_id].isoformat(sep=' ')}")

    # get start times
    starttimes: dict[str, datetime] = {}
    for tf_path in tf_dirs:
        starttimes[tf_path.name] = await get_creation_time_for_wf(rc, tf_path.name)


if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
