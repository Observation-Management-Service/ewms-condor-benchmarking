"""Get the runtimes for each EWMS workflow and the partner classical dag."""

import argparse
import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import plotext
from rest_tools.client import RestClient, SavedDeviceGrantAuth


class DidNotFinishException(Exception):
    """Raised when a workflow did not finish (ex: cancelled)."""


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__datetime__": obj.isoformat()}
        return super().default(obj)


def decode_datetime(obj):
    if "__datetime__" in obj:
        return datetime.fromisoformat(obj["__datetime__"])
    return obj


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

    if not times:
        raise DidNotFinishException()

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
        timeout=5 * 60,
        retries=0,
    )

    print(f"looking at {[str(d) for d in tf_dirs]}...")

    # parse dirs and query ewms
    runtimes: dict[str, tuple[datetime, datetime]] = {}
    for tf_path in tf_dirs:
        print(f"\n{tf_path.name}")

        try:
            end = get_final_time_for_taskforce(tf_path)
        except DidNotFinishException:
            print(f"-> did not finish -- so, no runtime")
            continue
        print(f"-> {end=}")
        start = await get_creation_time_for_wf(
            rc,
            tf_path.name.removeprefix("ewms-taskforce-"),
        )
        print(f"-> {start=}")
        runtimes[tf_path.name] = (start, end)
        print(f"RUNTIME: {end - start}")

    # Print sorted by taskforce ID
    # for tf_dname in sorted(runtimes):
    #     start, end = runtimes[tf_dname]
    #     print(
    #         f"{tf_dname} start={start.isoformat(sep=' ')} "
    #         f"end={end.isoformat(sep=' ')} "
    #         f"duration={end - start}"
    #     )

    return runtimes


async def get_classical_runtimes(
    runs_dirs: list[Path],
) -> dict[str, tuple[datetime, datetime]]:
    """Get the runtimes for each classical dag workflow."""
    dag_dirs = [
        d / "classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_None"
        for d in runs_dirs
    ]

    print(f"\nlooking at {[str(d) for d in dag_dirs]}...")

    # parse dirs and query ewms
    runtimes: dict[str, tuple[datetime, datetime]] = {}
    for dag in dag_dirs:
        name = f"{dag.parent.name}/{dag.name}"
        print(f"\n{name}")

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
        try:
            assert math.isclose(
                data["end_time"] - data["start_time"],
                data["duration"],
                abs_tol=1e-1,  # allowable float diff
            )
        except AssertionError:
            print(f"{(data["end_time"] - data["start_time"])=}")
            print(f"{data["duration"]=}")
            raise

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

        runtimes[name] = (start, end)
        print(f"RUNTIME: {end - start}")

    return runtimes


def match_side_by_sides(
    ewms_runtimes: dict[str, tuple[datetime, datetime]],
    classical_runtimes: dict[str, tuple[datetime, datetime]],
) -> list[tuple[str, str]]:
    """Match EWMS and classical runs by start time (within window)"""
    print(
        f"\nMatching runtimes (within 5 min) {len(ewms_runtimes)=} and {len(classical_runtimes)=}..."
    )
    window = 5 * 60
    used_classical = set()
    matches = []

    for ewms_name, (ewms_start, ewms_end) in sorted(ewms_runtimes.items()):
        best_match = None
        smallest_diff = float("inf")

        for class_name, (class_start, class_end) in sorted(classical_runtimes.items()):
            if class_name in used_classical:
                continue
            diff = abs((ewms_start - class_start).total_seconds())
            if diff < window and diff < smallest_diff:
                smallest_diff = diff
                best_match = class_name

        if best_match:
            used_classical.add(best_match)
            matches.append((ewms_name, best_match))
            class_start, class_end = classical_runtimes[best_match]
            print(f"\n🟢 {ewms_name} ↔ {best_match}")
            print(f"    EWMS     : {ewms_start} — {ewms_end} ({ewms_end - ewms_start})")
            print(
                f"    Classical: {class_start} — {class_end} ({class_end - class_start})"
            )
        else:
            print(
                f"\n🔴 {ewms_name} — no classical match found within {window/60} min ({ewms_start})"
            )

    for class_name in sorted(set(classical_runtimes) - used_classical):
        class_start, class_end = classical_runtimes[class_name]
        print(f"\n🔴 {class_name} — unmatched classical ({class_end})")

    return matches


def _fmt_td(seconds: float) -> str:
    """Format seconds as H:MM:SS.sss"""
    sign = "-" if seconds < 0 else ""
    td = timedelta(seconds=abs(seconds))
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    millis = td.microseconds // 1000
    return f"{sign}{hours}:{minutes:02}:{seconds:02}.{millis:03}"


def summarize_timedeltas(name: str, td_list: list[timedelta]) -> str:
    secs = np.array([td.total_seconds() for td in td_list])
    return (
        f"{name}:\n"
        f"  count : {len(td_list)}\n"
        f"  mean  : {_fmt_td(np.mean(secs))}\n"
        f"  std   : {_fmt_td(np.std(secs))}\n"
        f"  median: {_fmt_td(np.median(secs))}\n"
        f"  p25   : {_fmt_td(np.percentile(secs, 25))}\n"
        f"  p75   : {_fmt_td(np.percentile(secs, 75))}\n"
        f"  min   : {_fmt_td(np.min(secs))}\n"
        f"  max   : {_fmt_td(np.max(secs))}\n"
    )


def ascii_box_plot(arr: np.ndarray, width=40) -> str:
    """Return a text boxplot visualization using ├───■───┤ characters."""
    q1 = np.percentile(arr, 25)
    q2 = np.median(arr)
    q3 = np.percentile(arr, 75)
    min_ = arr.min()
    max_ = arr.max()

    def pos(val):
        return (
            int(round((val - min_) / (max_ - min_) * (width - 1)))
            if max_ > min_
            else width // 2
        )

    line = [" "] * width
    line[pos(min_)] = "│"
    line[pos(q1)] = "├"
    line[pos(q2)] = "■"
    line[pos(q3)] = "┤"
    line[pos(max_)] = "│"

    return "  boxplot   : " + "".join(line) + f"  [{min_:.2f} — {max_:.2f}]"


def summarize_ratios(name: str, vals: list[float]) -> str:
    arr = np.array(vals)

    def metrics(ratio: float) -> str:
        if ratio <= 0:
            return "invalid ratio"

        # Derived metrics
        pct_faster = (1 / ratio - 1) * 100  # e.g. 0.5 → 100% faster
        pct_slower = (ratio - 1) * 100  # e.g. 2.0 → 100% slower
        time_saved = (1 - ratio) * 100  # e.g. 0.6 → 40% time saved
        speedup = 1 / ratio  # e.g. 0.5 → 2x speedup
        log_speedup = np.log2(speedup)  # log₂ speedup

        # Directional wording
        if ratio < 1:
            dir_desc = f"{pct_faster:6.1f}% faster"
        elif ratio > 1:
            dir_desc = f"{pct_slower:6.1f}% slower"
        else:
            dir_desc = " equal speed     "

        return (
            f"{ratio:8.4f}  → {dir_desc}, "
            f"{time_saved:5.1f}% time saved, "
            f"{speedup:5.2f}× speedup, "
            f"log₂ speedup: {log_speedup:4.2f}"
        )

    mean = arr.mean()
    std = arr.std()
    median = np.median(arr)
    p25 = np.percentile(arr, 25)
    p75 = np.percentile(arr, 75)
    min_ = arr.min()
    max_ = arr.max()

    return (
        f"{name} (EWMS / Classical):\n"
        f"  count     : {len(vals)}\n"
        f"  mean      : {metrics(mean)}\n"
        f"  std dev   : {std:.4f}\n"
        f"  median    : {metrics(median)}\n"
        f"  p25       : {metrics(p25)}\n"
        f"  p75       : {metrics(p75)}\n"
        f"  min       : {metrics(min_)}\n"
        f"  max       : {metrics(max_)}\n"
    )


def compare_runtime_stats(
    ewms_runtimes: dict[str, tuple[datetime, datetime]],
    classical_runtimes: dict[str, tuple[datetime, datetime]],
    runtimes_side_by_side: list[tuple[str, str]],
):
    """Compare EWMS and classical runtimes for matched pairs."""
    print("\n📊 Runtime comparison stats:")

    diffs = []

    for ewms_name, class_name in runtimes_side_by_side:
        ewms_start, ewms_end = ewms_runtimes[ewms_name]
        class_start, class_end = classical_runtimes[class_name]

        ewms_dur = ewms_end - ewms_start
        class_dur = class_end - class_start
        ratio = (
            ewms_dur.total_seconds() / class_dur.total_seconds()
            if class_dur.total_seconds()
            else float("inf")
        )

        diffs.append(
            {
                "ewms": ewms_name,
                "classical": class_name,
                "ewms_duration": ewms_dur,
                "classical_duration": class_dur,
                "ratio": ratio,
                "abs_diff": ewms_dur - class_dur,
            }
        )

    if not diffs:
        print("No matched runs found. Skipping statistics.")
        return

    ewms_durations = [d["ewms_duration"] for d in diffs]
    classical_durations = [d["classical_duration"] for d in diffs]
    ratios = [d["ratio"] for d in diffs]
    abs_diffs = [d["abs_diff"] for d in diffs]

    print(summarize_timedeltas("EWMS durations", ewms_durations))
    print(summarize_timedeltas("Classical durations", classical_durations))
    print(summarize_timedeltas("EWMS - Classical (abs diff)", abs_diffs))

    print(summarize_ratios("EWMS/Classical ratio", ratios))
    plotext.clear_figure()
    plotext.box([ratios])
    plotext.title("EWMS vs Classical Runtime Ratios")
    plotext.plotsize(17, 20)
    plotext.show()

    with open("runtime-comparison.json", "w") as f:
        json.dump(
            [
                {
                    **d,
                    "ewms_duration": d["ewms_duration"].total_seconds(),
                    "classical_duration": d["classical_duration"].total_seconds(),
                    "abs_diff": d["abs_diff"].total_seconds(),
                }
                for d in diffs
            ],
            f,
            indent=2,
        )


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
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="include to not use any cache",
    )
    args = parser.parse_args()

    # classical
    cache_path = Path("./classical-runtimes.cache.json")
    if cache_path.exists() and not args.no_cache:
        with open(cache_path) as f:
            classical_runtimes = json.load(f, object_hook=decode_datetime)
    else:
        classical_runtimes = await get_classical_runtimes(
            _one_dir_or_many(args.classical, "runs_")
        )
        with open(cache_path, "w") as f:
            json.dump(classical_runtimes, f, cls=DateTimeEncoder)

    # ewms
    cache_path = Path("./ewms-runtimes.cache.json")
    if cache_path.exists() and not args.no_cache:
        with open(cache_path) as f:
            ewms_runtimes = json.load(f, object_hook=decode_datetime)
    else:
        ewms_runtimes = await get_ewms_runtimes(
            _one_dir_or_many(args.ewms, "ewms-taskforce-TF-")
        )
        with open(cache_path, "w") as f:
            json.dump(ewms_runtimes, f, cls=DateTimeEncoder)

    # figure runtimes stats
    runtimes_side_by_side = match_side_by_sides(ewms_runtimes, classical_runtimes)
    compare_runtime_stats(ewms_runtimes, classical_runtimes, runtimes_side_by_side)


if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
