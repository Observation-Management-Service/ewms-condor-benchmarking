"""Get the end times (time of last finished task) for each taskforce."""

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_final_time_for_taskforce(tf_path: Path) -> datetime | None:
    times: list[tuple[Path, datetime]] = []

    print(f"parsing {tf_path}...")

    for err_file in tf_path.rglob("*.err"):
        try:
            with err_file.open() as f:
                for line in f:
                    if "Done Tasking:" not in line:
                        continue
                    timestamp_str = line[:23]  # '2025-06-24 13:04:17.464'
                    try:
                        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                        times.append((Path(err_file), dt))
                    except ValueError:
                        raise
        except Exception as e:
            print(f"[WARN] Could not read {err_file}: {e}")

    # set timezones
    for i, (fpath, naive_dt) in enumerate(times):
        with open(fpath.with_suffix(".out"), "r") as f:
            for line in f:
                # ex: "║  Today:  2025-06-21 08:16:55+02:00"
                prefix = "║  Today: "
                if not line.startswith(prefix):
                    continue
                # print(line)
                time_str = line.removeprefix(prefix).strip().removesuffix("║").strip()
                # print(f"`{time_str}`")
                dt_with_tz = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S%z")
                # apply tz, then convert to UTC
                times[i] = (
                    fpath,
                    naive_dt.replace(tzinfo=dt_with_tz.tzinfo).astimezone(timezone.utc),
                )
                break  # stop reading this .out file

    # just_times = sorted(x[1] for x in times)
    # pprint.pprint(just_times)
    # pprint.pprint(just_times[0])
    # pprint.pprint(just_times[-1])
    return max(x[1] for x in times)


async def get_creation_time_for_wf(rc: RestClient, tf_id: str) -> datetime:
    # resp = await rc.request("GET", f"/v1/taskforces/{tf_id}")
    # print(json.dumps(resp, indent=2))

    def _to_workflow_id(tf_id: str) -> str:
        # TF-685643b1-7a50f872-ba2017b8-05bfe8b1 -> WF-685643b1-7a50f872
        parts = tf_id.split("-")
        return f"WF-{parts[1]}-{parts[2]}"

    workflow_id = _to_workflow_id(tf_id)
    resp = await rc.request("GET", f"/v1/workflows/{workflow_id}")

    return datetime.fromtimestamp(resp["timestamp"], tz=ZoneInfo("UTC"))


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

    # parse dirs and query ewms
    runtimes: dict[str, tuple[datetime, datetime]] = {}
    for tf_path in tf_dirs:
        final_time = get_final_time_for_taskforce(tf_path)
        print(f"-> {final_time=}")
        start_time = await get_creation_time_for_wf(
            rc,
            tf_path.name.removeprefix("ewms-taskforce-"),
        )
        print(f"-> {start_time=}")
        runtimes[tf_path.name] = (start_time, final_time)
        print(f"RUNTIME: {final_time - start_time}")

    # Print sorted by taskforce ID
    for tf_dname in sorted(runtimes):
        start, end = runtimes[tf_dname]
        print(
            f"{tf_dname} start={start.isoformat(sep=' ')} "
            f"end={end.isoformat(sep=' ')} "
            f"duration={end - start}"
        )


if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
