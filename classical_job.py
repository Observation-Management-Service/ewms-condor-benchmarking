"""A classical condor job that runs simulated tasks sequentially."""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Sequentially spawn tasks with Apptainer."""
    n_tasks = int(os.environ["TASKS_PER_JOB"])

    common_dir = os.path.abspath("./commondir")
    Path(common_dir).mkdir(exist_ok=True)

    for i in range(n_tasks):
        print(f"\n--- Launching task {i + 1}/{n_tasks} ---")
        try:
            subprocess.run(
                (
                    "apptainer run "
                    "--containall "  # don't auto-mount anything
                    "--no-eval "  # don't interpret CL args
                    #
                    f"--mount type=bind,source={common_dir},target=/commondir "
                    #
                    f"--env TASK_RUNTIME={os.environ['TASK_RUNTIME']} "
                    f"--env FAIL_PROB={os.environ['FAIL_PROB']} "
                    f"--env DO_TASK_RUNTIME_POISSON={os.environ['DO_TASK_RUNTIME_POISSON']} "
                    f"--env WORKER_SPEED_FACTOR={os.environ['WORKER_SPEED_FACTOR']} "
                    #
                    f"{os.environ['TASK_IMAGE']} "
                    # no args
                ).split(),
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"[FAIL] Task {i + 1} exited with {e.returncode}", file=sys.stderr)
            sys.exit(1)

    print(f"\n[SUMMARY] All {n_tasks} tasks succeeded.")


if __name__ == "__main__":
    main()
