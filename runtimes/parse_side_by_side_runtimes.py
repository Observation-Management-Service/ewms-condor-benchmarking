"""Get the runtimes for each EWMS workflow and the partner classical dag."""

import argparse
import asyncio
import json
import math
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
                    if "completed 0 task(s)" in line:
                        # ignore pilots that started after all tasks were done
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


async def get_ewms_runtimes(
    tf_dirs: list[Path],
) -> dict[str, tuple[datetime, datetime]]:
    """Get the runtimes for each EWMS workflow."""
    rc = SavedDeviceGrantAuth(
        "https://ewms-dev.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename=str(Path("~/ewms-dev-device-refresh-token").expanduser().resolve()),
        client_id="ewms-dev-public",
        retries=0,
    )

    print(f"looking at {[str(d) for d in tf_dirs]}...")

    # parse dirs and query ewms
    runtimes: dict[str, tuple[datetime, datetime]] = {}
    for tf_path in tf_dirs:
        end = get_final_time_for_taskforce(tf_path)
        print(f"-> {end=}")
        start = await get_creation_time_for_wf(
            rc,
            tf_path.name.removeprefix("ewms-taskforce-"),
        )
        print(f"-> {start=}")
        runtimes[tf_path.name] = (start, end)
        print(f"RUNTIME: {end - start}")

    # Print sorted by taskforce ID
    for tf_dname in sorted(runtimes):
        start, end = runtimes[tf_dname]
        print(
            f"{tf_dname} start={start.isoformat(sep=' ')} "
            f"end={end.isoformat(sep=' ')} "
            f"duration={end - start}"
        )

    return runtimes


async def get_classical_runtimes(
    runs_dirs: list[Path],
) -> dict[str, tuple[datetime, datetime]]:
    """Get the runtimes for each classical dag workflow."""
    dag_dirs = [
        d / "classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_None"
        for d in runs_dirs
    ]

    print(f"looking at {[str(d) for d in dag_dirs]}...")

    # parse dirs and query ewms
    runtimes: dict[str, tuple[datetime, datetime]] = {}
    for dag in dag_dirs:

        # Find the single metrics file
        metrics_file = list(dag.rglob("*.dag.metrics"))
        if not len(metrics_file) == 1:
            print(metrics_file)
            assert 0
        metrics_file = metrics_file[0]

        # Load and check
        with metrics_file.open() as f:
            data = json.load(f)

        # sanity check
        assert math.isclose(
            data["end_time"] - data["start_time"],
            data["duration"],
            abs_tol=1e-3,  # allowable float diff
        )

        end = datetime.fromtimestamp(
            data["end_time"],
            ZoneInfo("America/Chicago"),
        ).astimezone(timezone.utc)
        start = datetime.fromtimestamp(
            data["start_time"],
            ZoneInfo("America/Chicago"),
        ).astimezone(timezone.utc)

        print(f"-> {end=}")
        print(f"-> {start=}")

        runtimes[dag.name] = (start, end)
        print(f"RUNTIME: {end - start}")


def _one_dir_or_many(dpath: Path, many_prefix: str) -> list[Path]:
    dpath = dpath.resolve()
    if dpath.name.startswith(many_prefix):
        tf_dirs = [dpath]
    else:
        tf_dirs = [p for p in dpath.glob(f"{many_prefix}*") if p.is_dir()]
    return [p.resolve() for p in tf_dirs]


async def main():
    parser = argparse.ArgumentParser(
        description="Get the runtimes for each EWMS workflow and the partner classical dag."
    )
    parser.add_argument(
        "--ewms",
        required=True,
        type=Path,
        help="A directory containing multiple 'ewms-taskforce-TF-*' dirs",
    )
    parser.add_argument(
        "--classical",
        required=True,
        type=Path,
        help="A directory containing multiple condor logs for the classical runs",
    )
    args = parser.parse_args()

    # ewms_runtimes = await get_ewms_runtimes(
    #     _one_dir_or_many(args.ewms, "ewms-taskforce-TF-")
    # )
    classical_runtimes = await get_classical_runtimes(
        _one_dir_or_many(args.classical, "runs_")
    )


if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
