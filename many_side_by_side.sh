#!/bin/bash
set -euo pipefail

# Check for required argument
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <BENCHMARK_TAG>"
    exit 1
fi

BENCHMARK_TAG="$1"

# Base directory containing runs_*
base_dir="/scratch/eevans/ewms-benchmarking"
img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$BENCHMARK_TAG"

wait_for_no_jobs() {
    # waits for there to be no 'ewms'-user jobs
    while condor_q ewms -format '%d\n' ClusterId | read -r _; do
        echo "[WAIT] ewms has submitted Condor jobs. Waiting..."
        sleep 1800 # 30 mins
    done
}

# for each 'runs_*' dir:
for runs_dir in "${base_dir}"/runs_*; do

    if [[ -d $runs_dir ]]; then
        echo "Processing $runs_dir"
        cd "$runs_dir"

        # for each side-by-side flavor
        for X in A B C D; do
            echo "  Next up is $X"
            wait_for_no_jobs
            echo "  Running $img/app/run_side_by_side.sh $X in $runs_dir"
            "$img"/app/run_side_by_side.sh "$X"
        done

    fi

done
