#!/usr/bin/env python3
"""Build DAG files."""

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class TestVars:
    """Testing parameters."""

    N_JOBS: int
    TASKS_PER_JOB: int
    TASK_RUNTIME: int = 60
    FAIL_PROB: float = 0.0
    DO_TASK_RUNTIME_POISSON: str = "no"
    WORKER_SPEED_FACTOR: tuple[float, float] | None = None


def get_fname(num_jobs: int, test_vars: TestVars) -> str:
    """Assemble a file name from test_vars."""
    fname = f"ewms-sim-{num_jobs}"

    for k, v in asdict(test_vars).items():
        first_letters = "".join(word[0] for word in k.split("_"))
        fname = f"{fname}_{first_letters}_{v}"

    return f"{fname}.dag"


def write_dag_file(output_dir: Path, test_vars: TestVars):
    """Write the file."""
    n_digits = len(str(test_vars.N_JOBS))  # Auto-calculate padding width

    # figure filepath
    fpath = output_dir / get_fname(test_vars.N_JOBS, test_vars)
    if fpath.exists():
        raise FileExistsError(f"{fpath} already exists")

    # write!
    with open(fpath, "w") as f:
        # Write JOB lines with auto-zero-padding
        for i in range(1, test_vars.N_JOBS + 1):
            f.write(f"JOB {i:0{n_digits}d} ewms-sim.sub\n")

        f.write("\n")

        # Shared DEFAULT vars
        for key, value in asdict(test_vars).items():
            if key in ["N_JOBS"]:
                continue
            f.write(f'DEFAULT {key}="{value}"\n')
        f.write("\n")

        # Retry rule
        f.write("RETRY ALL_NODES 5 UNLESS-EXIT 0\n")


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Generate DAG fileS for EWMS simulation tests."
    )
    parser.add_argument(
        "--n-tasks",
        type=int,
        default=200_000,  # 200k
        help="Number of tasks to generate",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output DAG file name",
    )

    args = parser.parse_args()

    # prep tests
    all_tests = []
    for tpj in [1, 100]:
        if args.n_tasks % tpj:
            raise ValueError(
                f"Total number of tasks {args.n_tasks} must be divisible by {tpj}"
            )
        n_jobs = args.n_tasks / tpj
        all_tests.extend(
            [
                # fmt: off
                TestVars(N_JOBS=n_jobs, TASKS_PER_JOB=tpj),
                TestVars(N_JOBS=n_jobs, TASKS_PER_JOB=tpj, FAIL_PROB=0.01),
                TestVars(N_JOBS=n_jobs, TASKS_PER_JOB=tpj, DO_TASK_RUNTIME_POISSON="yes"),
                TestVars(N_JOBS=n_jobs, TASKS_PER_JOB=tpj, WORKER_SPEED_FACTOR=(1.0, 5.0)),
                # fmt: on
            ],
        )

    # write dags
    for test_vars in all_tests:
        write_dag_file(args.output_dir, test_vars)


if __name__ == "__main__":
    main()
