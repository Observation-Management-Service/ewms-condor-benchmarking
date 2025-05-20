#!/usr/bin/env python3
"""Build DAG files."""

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path

TOTAL_TASKS = 200_000


@dataclass
class TestVars:
    TASK_RUNTIME: int
    FAIL_PROB: float
    DO_TASK_RUNTIME_POISSON: str
    DO_WORKER_SPEED_FACTOR: str


def get_fname(num_jobs: int, test_vars: TestVars) -> str:
    """Assemble a file name from test_vars."""
    fname = f"ewms-sim-{num_jobs}"

    for k, v in asdict(test_vars).items():
        first_letters = "".join(word[0] for word in k.split("_"))
        fname = f"{fname}_{first_letters}_{v}"

    return f"{fname}.dag"


def write_dag_file(output_dir: Path, num_jobs: int, test_vars: TestVars):
    """Write the file."""
    n_digits = len(str(num_jobs))  # Auto-calculate padding width

    if TOTAL_TASKS % num_jobs:
        raise ValueError(f"Number of tasks {num_jobs} must be divisible by {num_jobs}")

    # figure filepath
    fpath = output_dir / get_fname(num_jobs, test_vars)
    if fpath.exists():
        raise FileExistsError(f"{fpath} already exists")

    # write!
    with open(fpath, "w") as f:
        # Write JOB lines with auto-zero-padding
        for i in range(1, num_jobs + 1):
            f.write(f"JOB {i:0{n_digits}d} ewms-sim.sub\n")

        f.write("\n")

        # Shared DEFAULT vars
        for key, value in asdict(test_vars).items():
            f.write(f'DEFAULT {key}="{value}"\n')

        f.write(f'DEFAULT N_TASKS="{int(TOTAL_TASKS/num_jobs)}"\n')
        f.write("\n")

        # Retry rule
        f.write("RETRY ALL_NODES 5 UNLESS-EXIT 0\n")


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Generate DAG fileS for EWMS simulation tests."
    )
    parser.add_argument(
        "-n",
        "--num-jobs",
        type=int,
        required=True,
        help="Number of jobs to generate",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output DAG file name",
    )

    args = parser.parse_args()

    all_tests = [
        TestVars(
            TASK_RUNTIME=60,
            FAIL_PROB=0.0,
            DO_TASK_RUNTIME_POISSON="no",
            DO_WORKER_SPEED_FACTOR="no",
        ),
        ...,  # TODO
    ]
    for test_vars in all_tests:
        write_dag_file(args.output_dir, args.num_jobs, test_vars)


if __name__ == "__main__":
    main()
