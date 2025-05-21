#!/usr/bin/env python3
"""Build test submission files -- ewms and condor (dagman)."""

import argparse
import json
import logging
from dataclasses import asdict, dataclass, fields
from pathlib import Path


LOGGER = logging.getLogger(__name__)

SUBMIT_FNAME = "ewms-sim.submit"

EWMS_N_WORKERS = 2_000

MAX_WORKER_RUNTIME = 60 * 60
N_CORES = 1
PRIORITY = 100
WORKER_DISK = "1GB"
WORKER_MEMORY = "512M"

# copied from https://github.com/Observation-Management-Service/ewms-task-management-service/blob/v1.0.10/tests/unit/test_starter.py#L53
REQUIREMENTS_EWMS_SETS = (
    "ifthenelse(!isUndefined(HAS_SINGULARITY), HAS_SINGULARITY, HasSingularity) && "
    "HAS_CVMFS_icecube_opensciencegrid_org && "
    # "has_avx && has_avx2 && "
    '(OSG_OS_VERSION =?= "8" || OSG_OS_VERSION =?= "9") && '
    "SingularityUserNamespaces =?= true && "
    'GLIDEIN_Site =!= "AMNH" && '
    'GLIDEIN_Site =!= "Kansas State University" && '
    'GLIDEIN_Site =!= "NotreDame" && '
    'GLIDEIN_Site =!= "Rhodes-HPC" && '
    'GLIDEIN_Site =!= "SDSC-PRP" && '
    'GLIDEIN_Site =!= "SU-ITS" && '
    'GLIDEIN_Site =!= "San Diego Supercomputer Center" && '
    'OSG_SITE_NAME =!= "Wichita State University" '  # &&
)


@dataclass
class TestVars:
    """Testing parameters."""

    TASKS_PER_JOB: int | str

    TASK_RUNTIME: int = 60
    FAIL_PROB: float = 0.0
    DO_TASK_RUNTIME_POISSON: str = "no"
    WORKER_SPEED_FACTOR: tuple[float, float] | None = None


def get_fname(prefix: str, test_vars: TestVars, ext: str) -> str:
    """Assemble a file name from test_vars."""
    fname = f"{prefix}"

    for k, v in asdict(test_vars).items():
        first_letters = "".join(word[0] for word in k.split("_"))
        fname = f"{fname}_{first_letters}_{v}"

    return f"{fname}.{ext.lstrip(".")}"


class DAGBuilder:
    """For building the DAG things."""

    @staticmethod
    def write_dag_file(output_dir: Path, test_vars: TestVars, n_jobs: int) -> Path:
        """Write the file."""

        # figure filepath
        fpath = output_dir / get_fname("ewms_sim_classical", test_vars, ".dag")
        if fpath.exists():
            raise FileExistsError(f"{fpath} already exists")

        # write!
        with open(fpath, "w") as f:
            # Write JOB lines with auto-zero-padding
            n_digits = len(str(n_jobs))  # Auto-calculate padding width
            for i in range(1, n_jobs + 1):
                f.write(f"JOB {i:0{n_digits}d} {output_dir/SUBMIT_FNAME}\n")

            f.write("\n")

            # Shared DEFAULT vars
            for key, value in asdict(test_vars).items():
                f.write(f'DEFAULT {key}="{value}"\n')
            f.write("\n")

            # Retry rule
            f.write("RETRY ALL_NODES 5 UNLESS-EXIT 0\n")

        return fpath

    @staticmethod
    def write_submit_file(output_dir: Path, task_image: Path) -> None:
        """Write a condor submit file."""
        env_vars = [x for x in fields(TestVars) if x not in ["N_JOBS"]]

        contents = f"""
universe                   = container
+should_transfer_container = no
container_image            = {task_image}

# must support same reqs as ewms in order to compare scheduling
Requirements               = {REQUIREMENTS_EWMS_SETS}

+FileSystemDomain          = "blah"  # must be quoted 

log                        = /scratch/.../$(clusterid).log

should_transfer_files      = YES
when_to_transfer_output    = ON_EXIT_OR_EVICT
transfer_executable        = false

request_cpus               = {N_CORES}
request_memory             = {WORKER_MEMORY}
request_disk               = {WORKER_DISK}

priority                   = {PRIORITY}
+WantIOProxy               = true  # for HTChirp (ewms)
+OriginalTime              = 3600  # Execution time limit -- 1 hour default on OSG

# pass in all DAG-defined vars as environment
environment = "{" ".join(f'{v}=$({v})' for v in env_vars)}" 

queue 1
        """
        with open(output_dir / SUBMIT_FNAME, "w") as f:
            f.write(contents)


class EWMSRequestBuilder:
    """For building the EWMS things."""

    @staticmethod
    def write_request_json(
        output_dir: Path,
        test_vars: TestVars,
        task_image: Path,
    ) -> Path:
        """Write a JSON file used for requesting an ewms workflow."""

        # figure filepath
        fpath = output_dir / get_fname("ewms_workflow", test_vars, ".json")
        if fpath.exists():
            raise FileExistsError(f"{fpath} already exists")

        post_body = {
            "public_queue_aliases": ["input-queue", "output-queue"],
            "tasks": [
                {
                    "cluster_locations": ["sub-2"],
                    "in_queue_aliases": ["input-queue"],
                    "out_queue_aliases": ["output-queue"],
                    "task_image": task_image,
                    "task_args": "",
                    "task_env": {
                        k: str(v).lower()
                        for k, v in asdict(test_vars).items()
                        if k not in ["N_JOBS"]
                    },
                    "n_workers": EWMS_N_WORKERS,
                    "worker_config": {
                        "condor_requirements": "",  # no extra needs since ewms sets several already
                        "do_transfer_worker_stdouterr": False,  # same as .submit file
                        "max_worker_runtime": MAX_WORKER_RUNTIME,
                        "n_cores": N_CORES,
                        "priority": PRIORITY,
                        "worker_disk": WORKER_DISK,
                        "worker_memory": WORKER_MEMORY,
                    },
                }
            ],
        }

        with open(fpath, "w") as f:
            f.write(json.dumps(post_body))

        return fpath


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Generate DAG fileS for EWMS simulation tests."
    )
    parser.add_argument(
        "--n-tasks",
        type=int,
        default=200_000,  # 200k
        help="Total number of tasks to generate",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to put submission files",
    )
    parser.add_argument(
        "--task-image",
        type=Path,
        required=True,
        help="File path to the apptainer image used for each task",
    )

    args = parser.parse_args()

    # all dags share same submit file
    DAGBuilder.write_submit_file(args.output_dir, args.task_image)

    # prep tests
    for tasks_per_job in [1, 100, "ewms"]:
        LOGGER.info(f"making tests for {tasks_per_job=}")
        test_vars = [
            TestVars(TASKS_PER_JOB=tasks_per_job),
            TestVars(TASKS_PER_JOB=tasks_per_job, FAIL_PROB=0.01),
            TestVars(TASKS_PER_JOB=tasks_per_job, DO_TASK_RUNTIME_POISSON="yes"),
            TestVars(TASKS_PER_JOB=tasks_per_job, WORKER_SPEED_FACTOR=(1.0, 5.0)),
        ]

        # ewms
        if tasks_per_job == "ewms":
            for tv in test_vars:
                fpath = EWMSRequestBuilder.write_request_json(
                    args.output_dir, tv, args.task_image
                )
                LOGGER.info(f"generated {fpath=}")
        # classical condor/dagman
        else:
            if args.n_tasks % tasks_per_job:
                raise ValueError(
                    f"Total number of tasks {args.n_tasks} must be divisible by {tasks_per_job}"
                )
            n_jobs = int(args.n_tasks / tasks_per_job)
            # write dags
            for tv in test_vars:
                fpath = DAGBuilder.write_dag_file(args.output_dir, tv, n_jobs)
                LOGGER.info(f"generated {fpath=}")


if __name__ == "__main__":
    main()
    LOGGER.info("Done.")
