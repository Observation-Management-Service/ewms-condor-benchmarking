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
screen -dmS ewms_benchmarking_many bash -c "${img}/app/many_side_by_side.sh ${BENCHMARK_TAG} >> /scratch/eevans/ewms_benchmarking_many.log 2>&1"
```

## Calculating Runtimes for Benchmarking

## Runtime (Workflow) Calculation

| System        | Start Time                  | End Time                                   | Notes                                 |
|---------------|-----------------------------|--------------------------------------------|---------------------------------------|
| Classical DAG | `submit time` (in job log)  | `time of last job finish`                  | get from job event log                |
| EWMS          | `request time` (in EWMS DB) | `time of last output-message sent` ( logs) | `grep` cluster job logs for `"Chirp"` |

---

### Notes on Inputs/Outputs

The intention of the benchmarks is to measure scheduling, not inputs and outputs.

### Classical DAG: No inputs or outputs

- We're giving each job the "simulated events" via env vars, so no file transfer needed
- This way, we're focussing only on scheduling

### EWMS: No outputs, just inputs

- EWMS needs events in order to run
- EWMS has an output queue — we're just not looking at its contents
- This way, no long-running external process needed for EWMS MQ receiver
