"""Build test submission files -- ewms and condor (dagman)."""

import argparse
import json
import logging
import os
import shutil
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

SUBMIT_FNAME = "ewms-sim.submit"

SCRATCH_DIR = Path(
    os.getenv(
        "EWMS_BENCHMARKING_SCRATCH_DIR_OVERRIDE",
        "/scratch/eevans/ewms-benchmarking",
    )
)

# fmt: off
CLASSICAL_PREFIX = "classical_dag"
EWMS_PREFIX      = "ewms_workflow"  # same length so filepaths look good
# fmt: on

N_DIGITS_FNAME = 4

EWMS_N_WORKERS = 2_000

# see https://portal.osg-htc.org/documentation/htc_workloads/workload_planning/jobdurationcategory/
MAX_WORKER_RUNTIME = 60 * 60 * 2  # 20 hours
#
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
    # "SingularityUserNamespaces =?= true && "  # TODO: this was preventing runs, needed?
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
    DO_TASK_RUNTIME_POISSON: str = "n"
    WORKER_SPEED_FACTOR: tuple[float, float] | None = None


def get_fname(prefix: str, vars: dict[str, Any], suffix: str) -> str:
    """Assemble a filename from vars, with components padded for reasonable good looks."""
    middle_parts = []

    for key, val in vars.items():
        first_letters = "".join(word[0] for word in key.split("_")).upper()

        # Convert value to str, handling list/tuple specially
        if isinstance(val, (tuple, list)):
            str_val = "_".join(str(x) for x in val)
        else:
            str_val = str(val)

        # Normalize value
        # -> an int?
        if str_val.isdigit():
            str_val = f"{int(str_val):0{N_DIGITS_FNAME}d}"
        # -> a float?
        else:
            try:
                float(str_val)
            except ValueError:
                pass
            else:  # put zeros on right to make up spacing
                if (nzeros := N_DIGITS_FNAME - len(str_val)) > 0:
                    str_val += "0" * nzeros

        # attach
        middle_parts.append(f"{first_letters}_{str_val}")

    middle_string = "__".join(middle_parts)
    return f"{prefix}__{middle_string}{suffix}"


class DAGBuilder:
    """For building the DAG things."""

    @staticmethod
    def write_dag_file(output_dir: Path, test_vars: TestVars, n_jobs: int) -> Path:
        """Write the file."""

        # figure filepath
        fpath = output_dir / get_fname(CLASSICAL_PREFIX, asdict(test_vars), ".dag")
        if fpath.exists():
            raise FileExistsError(f"{fpath} already exists")

        # write!
        with open(fpath, "w") as f:
            # Write JOB lines with auto-zero-padding
            n_digits = len(str(n_jobs))  # Auto-calculate padding width
            for i in range(1, n_jobs + 1):
                # dagjob
                jobid = f"J{i:0{n_digits}d}"
                f.write(f"JOB {jobid} {output_dir/SUBMIT_FNAME}\n")
                # vars -- these are the same for all dagjobs
                var_str = " ".join(f'{k}="{v}"' for k, v in asdict(test_vars).items())
                var_str += f' LOG_FNAME_NOEXT="{fpath.stem}"'
                f.write(f"VARS {jobid} {var_str}\n")

            f.write("\n")

            # Retry rule
            f.write("RETRY ALL_NODES 5 UNLESS-EXIT 0\n")

        return fpath

    @staticmethod
    def write_submit_file(output_dir: Path, task_image: Path) -> None:
        """Write a condor submit file."""
        test_vars_names = [x.name for x in fields(TestVars)]

        env_vars = [f"{v}=$({v})" for v in test_vars_names]
        env_vars.append(f"TASK_IMAGE={task_image}")

        contents = f"""
universe                   = container
+should_transfer_container = no
# we're using the same image for both the job and the task -- simple
container_image            = {task_image}
# override the container's default CMD (see dockerfile) 
arguments                  = python /app/classical_job.py

# must support same reqs as ewms in order to compare scheduling
Requirements               = {REQUIREMENTS_EWMS_SETS}

# must be quoted
+FileSystemDomain          = "blah" 

log                        = {SCRATCH_DIR / "$(LOG_FNAME_NOEXT).$(DAG_NODE_NAME).$(clusterid).log"}

output                     = {SCRATCH_DIR / "$(LOG_FNAME_NOEXT).$(DAG_NODE_NAME).$(clusterid).$(Process).out"}
error                      = {SCRATCH_DIR / "$(LOG_FNAME_NOEXT).$(DAG_NODE_NAME).$(clusterid).$(Process).err"}

should_transfer_files      = YES
when_to_transfer_output    = ON_EXIT_OR_EVICT
transfer_executable        = false

request_cpus               = {N_CORES}
request_memory             = {WORKER_MEMORY}
request_disk               = {WORKER_DISK}

priority                   = {PRIORITY}

# for HTChirp (ewms does this)
+WantIOProxy               = true

# execution time limit -- 1 hour default on OSG
+OriginalTime              = {MAX_WORKER_RUNTIME}  

# pass in all DAG-defined vars as environment
environment = "{" ".join(env_vars)}" 

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
        fpath = output_dir / get_fname(EWMS_PREFIX, asdict(test_vars), ".json")
        if fpath.exists():
            raise FileExistsError(f"{fpath} already exists")

        post_body = {
            "public_queue_aliases": ["input-queue", "output-queue"],
            "tasks": [
                {
                    "cluster_locations": ["sub-2"],
                    "in_queue_aliases": ["input-queue"],
                    "out_queue_aliases": ["output-queue"],
                    "task_image": str(task_image),
                    "task_args": "",
                    "task_env": {
                        k: str(v).lower()
                        for k, v in asdict(test_vars).items()
                        if k not in ["N_JOBS"]
                    },
                    "n_workers": EWMS_N_WORKERS,
                    "worker_config": {
                        "condor_requirements": "",  # no extra needs since ewms sets several already
                        "do_transfer_worker_stdouterr": True,  # same as .submit file
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
        "--task-image",
        type=Path,
        required=True,
        help="File path to the apptainer image used for each task",
    )
    args = parser.parse_args()

    if not args.task_image.exists():
        raise FileNotFoundError(args.task_image)

    # prep SCRATCH_DIR
    if not SCRATCH_DIR.is_dir():
        raise NotADirectoryError(SCRATCH_DIR)
    if list(SCRATCH_DIR.iterdir()):
        raise RuntimeError(f"{SCRATCH_DIR=} must be an empty directory")

    # all dags share same submit file
    DAGBuilder.write_submit_file(SCRATCH_DIR, args.task_image)

    # prep tests
    for tasks_per_job in [1, 100, "ewms"]:
        LOGGER.info(f"making tests for {tasks_per_job=}")
        test_vars = [
            TestVars(TASKS_PER_JOB=tasks_per_job),
            TestVars(TASKS_PER_JOB=tasks_per_job, FAIL_PROB=0.01),
            TestVars(TASKS_PER_JOB=tasks_per_job, DO_TASK_RUNTIME_POISSON="y"),
            TestVars(TASKS_PER_JOB=tasks_per_job, WORKER_SPEED_FACTOR=(1.0, 5.0)),
        ]

        # ewms
        if tasks_per_job == "ewms":
            for tv in test_vars:
                fpath = EWMSRequestBuilder.write_request_json(
                    SCRATCH_DIR, tv, args.task_image
                )
                LOGGER.info(f"generated {str(fpath)}")
        # classical condor/dagman
        else:
            if args.n_tasks % tasks_per_job:
                raise ValueError(
                    f"Total number of tasks {args.n_tasks} must be divisible by {tasks_per_job}"
                )
            n_jobs = int(args.n_tasks / tasks_per_job)
            # write dags
            for tv in test_vars:
                fpath = DAGBuilder.write_dag_file(SCRATCH_DIR, tv, n_jobs)
                LOGGER.info(f"generated {str(fpath)}")

    # mkdir test dirs
    for n in range(1, 15):
        test_dir = SCRATCH_DIR / f"test_{n:02d}"
        os.mkdir(test_dir)
        # mkdir a dir for each dag
        for f in SCRATCH_DIR.iterdir():
            if f.suffix != ".dag":
                continue
            subtest_dir = test_dir / f.stem
            os.mkdir(subtest_dir)
            shutil.copy(f, subtest_dir / f.name)

    # "ls" SCRATCH_DIR
    LOGGER.info(f"ls {SCRATCH_DIR}")
    for f in SCRATCH_DIR.iterdir():
        LOGGER.info(f)


if __name__ == "__main__":
    main()
    LOGGER.info("Done.")
