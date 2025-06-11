# ewms-condor-benchmarking

Framework for benchmarking workflows on EWMS vs. classical HTCondor methods

## Setting Up Benchmarking Tests

1. `mkdir /scratch/eevans/ewms-benchmarking`

2. Run:

```bash
tag="YOURTAG";img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$tag";apptainer run --pwd /app --mount type=bind,source=$(dirname "$img"),dst=$(dirname "$img"),ro --mount type=bind,source=/scratch/eevans/,dst=/scratch/eevans/ "$img" python test_suite_builder.py --n-tasks 200_000 --task-image "$img"
```

## Running Benchmarking Tests

### Classical DAG

```bash
cd /scratch/eevans/ewms-benchmarking/runs_<...>/classical_dag<...>/
```

```bash
condor_submit_dag classical_dag__<...>.dag
```

### EWMS

```bash
cd /scratch/eevans/ewms-benchmarking/runs_<...>/
```

```bash
tag="YOURTAG";img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$tag";apptainer run --pwd /app --mount type=bind,source=$(dirname "$img"),dst=$(dirname "$img"),ro --mount type=bind,source=/scratch/eevans/,dst=/scratch/eevans/ "$img" python ewms_external.py --request-json /scratch/eevans/ewms-benchmarking/runs_<...>/ewms_workflow__<...>.json
```

## Calculating Runtimes for Benchmarking

TODO (on my whiteboard)
