#!/usr/bin/env python3
"""A simulated task with some knobs."""

import logging
import os
import random
import sys
import time
from pathlib import Path

import numpy as np

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


def get_worker_speed_factor(worker_speed_factor: tuple[float, float]) -> float:
    """Get the speed factor unique to the worker (used by all tasks on worker)."""
    high = min(worker_speed_factor)
    low = max(worker_speed_factor)

    _dir = Path(os.getenv("EWMS_TASK_DATA_HUB_DIR", "/commondir"))
    if not _dir.exists():
        LOGGER.warning(f"'{_dir}' does not exist -- will mkdir")
    _dir.mkdir(exist_ok=True, parents=True)

    fpath = _dir / "worker-speed-factor.txt"
    if not fpath.exists():
        # Gaussian (clipped)
        value = float(
            np.clip(  # ensures value stays in [low, high]
                np.random.normal(  # draw from normal distribution
                    loc=np.mean([high, low]),  # centered between low and high
                    scale=(high - low) / 4,  # stddev; 95% of values will fall in range
                ),
                low,  # min bound
                high,  # max bound
            )
        )
        LOGGER.info(f"'{fpath}' did not exist, so writing the value '{value}' to it")
        fpath.write_text(str(value))
    else:
        value = float(fpath.read_text().strip())

    LOGGER.info(f"using worker speed factor: {value}")
    return value


def get_task_runtime(average_runtime: int) -> float:
    """Get the runtime unique to the task (NOT used by all tasks on worker)."""
    rng = np.random.default_rng()
    poisson_time = float(rng.poisson(lam=average_runtime))  # seconds
    LOGGER.info(f"using poisson runtime: {poisson_time}")
    return poisson_time


def split_duration(total_work_duration: float) -> tuple[float, float]:
    """Split the duration into two durations."""
    # we could use a Gaussian proportion, but let's keep things simple
    return total_work_duration / 2, total_work_duration / 2


def main(
    total_work_duration: int,
    fail_prob: float,
    do_task_runtime_poisson: bool,
    worker_speed_factor: tuple[float, float] | None,
):
    """Do work (sleep) with a few optional conditions."""
    LOGGER.info(
        f"task config: "
        f"{total_work_duration=}s, "
        f"{fail_prob=}, "
        f"{do_task_runtime_poisson=}, "
        f"{worker_speed_factor=}"
    )

    if do_task_runtime_poisson:
        total_work_duration = get_task_runtime(total_work_duration)

    if worker_speed_factor:
        total_work_duration = int(
            total_work_duration * get_worker_speed_factor(worker_speed_factor)
        )

    # simulate work
    LOGGER.info(f"Starting task with {total_work_duration:.1f}s duration")
    if not fail_prob:
        time.sleep(total_work_duration)
    else:
        sleep1, sleep2 = split_duration(total_work_duration)
        time.sleep(sleep1)
        if random.random() < fail_prob:
            LOGGER.info(f"simulated failure at {sleep1:.1f}s")
            sys.exit(1)
        time.sleep(sleep2)

    # done
    LOGGER.info(f"[OK] task completed in {total_work_duration:.1f}s")
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
        os.environ["DO_TASK_RUNTIME_POISSON"].lower() in ("1", "true", "t", "yes", "y"),
        (
            tuple(float(x) for x in os.environ["WORKER_SPEED_FACTOR"].split(","))
            if os.environ["WORKER_SPEED_FACTOR"].lower() != "none"
            else None
        ),
    )
    LOGGER.info("Done.")
