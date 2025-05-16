#!/usr/bin/env python3
import os
import subprocess
import sys


def main(n_tasks, work_duration_min, work_duration_max, fail_prob):
    for i in range(n_tasks):
        print(f"\n--- Launching task {i + 1}/{n_tasks} ---")
        try:
            subprocess.run(
                (
                    "apptainer run "
                    "--containall "  # don't auto-mount anything
                    "--no-eval "  # don't interpret CL args
                    f"--env WORK_DURATION_MIN={work_duration_min} "
                    f"--env WORK_DURATION_MAX={work_duration_max} "
                    f"--env FAIL_PROB={fail_prob} "
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
    main(
        int(os.environ["N_TASKS_IN_BUNDLE"]),
        float(os.environ["WORK_DURATION_MIN"]),
        float(os.environ["WORK_DURATION_MAX"]),
        float(os.environ["FAIL_PROB"]),
    )
