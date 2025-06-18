#!/bin/bash
set -euo pipefail

if [[ "$(whoami)" != "ewms" ]]; then
    echo "Error: This script must be run as user 'ewms'." >&2
    exit 1
fi

if [[ $PWD != /scratch/eevans/ewms-benchmarking/runs_* ]]; then
    echo "Error: Must be in /scratch/eevans/ewms-benchmarking/runs_*" >&2
    exit 1
fi

if [[ -z ${BENCHMARK_TAG:-} ]]; then
    echo "Error: BENCHMARK_TAG is not set." >&2
    exit 1
fi

########################################################################################################################
# Run benchmarks side-by-side to use same conditions

# classical dag (very quick command)
cd classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_1.0_5.0
condor_submit_dag classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_1.0_5.0.dag
# now, ewms (takes a minute or so)
cd ..
img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$BENCHMARK_TAG"
apptainer run --pwd /app \
    --mount type=bind,source=$(dirname "$img"),dst=$(dirname "$img"),ro \
    --mount type=bind,source=/scratch/eevans/,dst=/scratch/eevans/ \
    "$img" python ewms_external.py \
    --request-json $PWD/ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_n__WSF_1.0_5.0.json \
    --n-tasks 200_000

# TODO: do same for the following... make a function

classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_None
ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_n__WSF_None.json

classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_y__WSF_None
ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_y__WSF_None.json

classical_dag__TPJ_0100__TR_0060__FP_0.01__DTRP_n__WSF_None
ewms_workflow__TPJ_ewms__TR_0060__FP_0.01__DTRP_n__WSF_None.json
