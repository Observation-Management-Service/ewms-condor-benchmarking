# ewms-condor-benchmarking

Framework for benchmarking workflows on EWMS vs. classical HTCondor methods

## Setting Up Benchmarking Tests

1. `mkdir /scratch/eevans/ewms-benchmarking`

2. Run:

```bash
BENCHMARK_TAG="YOURTAG";img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$BENCHMARK_TAG";apptainer run --pwd /app --mount type=bind,source=$(dirname "$img"),dst=$(dirname "$img"),ro --mount type=bind,source=/scratch/eevans/,dst=/scratch/eevans/ "$img" python test_suite_builder.py --n-tasks 200_000 --task-image "$img"
```

## Running Benchmarking Tests

See [run_side_by_side.sh](run_side_by_side.sh)

### Running Many

Also:

```bash
screen -dmS ewms_benchmarking_many bash -c "${img}/app/many_side_by_side.sh >> /scratch/eevans/ewms_benchmarking_many.log 2>&1"
```

## Calculating Runtimes for Benchmarking

TODO (on my whiteboard)
