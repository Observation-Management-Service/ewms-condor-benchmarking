#!/usr/bin/env python3
"""A simulated task with some knobs."""

import os
import random
import sys
import time
from pathlib import Path

import numpy as np


def get_worker_speed_factor() -> float:
    """Get the speed factor unique to the worker (used by all tasks on worker)."""
    fpath = Path("WORKER_SPEED_FACTOR")  # TODO: store in an accessible place

    if not fpath.exists():
        value = 0.0  # TODO: generate # guassian
        with open(fpath, "w") as f:
            fpath.write_text(str(value))
    else:
        with open(fpath) as f:
            value = float(f.read().strip())

    return value


def get_task_runtime(average_runtime: int) -> float:
    """Get the runtime unique to the task (NOT used by all tasks on worker)."""
    # poisson
    rng = np.random.default_rng(None)
    poisson_time = rng.poisson(lam=average_runtime)  # seconds
    return poisson_time


def split_duration(total_work_duration: int) -> tuple[float, float]:
    """Split the duration into two durations."""
    # we could use a Gaussian proportion, but let's keep things simple
    return total_work_duration / 2, total_work_duration / 2


def main(
    total_work_duration: int,
    fail_prob: float,
    do_task_runtime_poisson: bool,
    do_worker_speed_factor: bool,
):
    """Do work (sleep) with a few optional conditions."""

    if do_task_runtime_poisson:
        total_work_duration = get_task_runtime(total_work_duration)

    if do_worker_speed_factor:
        total_work_duration = int(total_work_duration * get_worker_speed_factor())

    # simulate work
    if not fail_prob:
        time.sleep(total_work_duration)
    else:
        # w/ potential failure part-way through work
        first_portion, second_portion = split_duration(total_work_duration)
        time.sleep(first_portion)
        if random.random() < fail_prob:
            print(f"[FAIL] Simulated failure at {first_portion:.1f}s", file=sys.stderr)
            sys.exit(1)
        time.sleep(second_portion)

    # done
    print(f"[OK] Task completed in {total_work_duration:.1f}s")
    # -> if this is an ewms task, write an output so the pilot can forward it on as an event
    if "EWMS_TASK_INFILE" in os.environ:
        with open(os.environ["EWMS_TASK_INFILE"]) as f:
            contents = f.read()
        with open(os.environ["EWMS_TASK_OUTFILE"], "w") as f:
            f.write(contents)


if __name__ == "__main__":
    main(
        int(os.environ["TASK_RUNTIME"]),
        float(os.environ["FAIL_PROB"]),
        os.environ["DO_TASK_RUNTIME_POISSON"].lower() in ("1", "true", "t", "yes"),
        os.environ["DO_WORKER_SPEED_FACTOR"].lower() in ("1", "true", "t", "yes"),
    )
