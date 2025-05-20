#!/usr/bin/env python3
"""Build a DAG file."""

import argparse
from pathlib import Path
from typing import Any


def get_fname(num_jobs: int, test_vars: dict[str, Any]) -> str:
    """Assemble a file name from test_vars."""
    fname = f"ewms-sim-{num_jobs}"

    for k, v in sorted(test_vars.items()):
        first_letters = "".join(word[0] for word in k.split("_"))
        fname = f"{fname}_{first_letters}_{v}"

    return f"{fname}.dag"


def write_dag_file(
    output_dir: Path,
    num_jobs: int,
    test_vars: dict[str, Any],
):
    """Write the file."""
    n_digits = len(str(num_jobs))  # Auto-calculate padding width

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
        for key, value in test_vars.items():
            f.write(f'DEFAULT {key}="{value}"\n')
        f.write("\n")

        # Retry rule
        f.write("RETRY ALL_NODES 5 UNLESS-EXIT 0\n")


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Generate a DAG file for EWMS simulation jobs."
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

    test_vars = {
        "TASK_RUNTIME": 60,
        "FAIL_PROB": 0.0,
        "DO_TASK_RUNTIME_POISSON": "yes",
        "DO_WORKER_SPEED_FACTOR": "yes",
    }

    args = parser.parse_args()
    write_dag_file(args.output_dir, args.num_jobs, test_vars)


if __name__ == "__main__":
    main()
