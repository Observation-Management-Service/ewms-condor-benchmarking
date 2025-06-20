#!/bin/bash
set -euo pipefail

########################################################################################################################
# Run benchmarks side-by-side to use same conditions
########################################################################################################################

readonly SCRATCH_DIR="/scratch/eevans"

if [[ "$(whoami)" != "ewms" ]]; then
    echo "Error: This script must be run as user 'ewms'." >&2
    exit 1
fi

if [[ $PWD != "$SCRATCH_DIR"/ewms-benchmarking/runs_* ]]; then
    echo "Error: Must be in $SCRATCH_DIR/ewms-benchmarking/runs_*" >&2
    exit 1
fi

if [[ -z ${BENCHMARK_TAG:-} ]]; then
    echo "Error: BENCHMARK_TAG is not set." >&2
    exit 1
fi

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 [A|B|C|D]" >&2
    exit 1
fi

########################################################################################################################
# run a pair

choice="$1"
case "$choice" in
    A)
        classical="classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_None"
        ewms_json="ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_n__WSF_None.json"
        ;;
    B)
        classical="classical_dag__TPJ_0100__TR_0060__FP_0.01__DTRP_n__WSF_None"
        ewms_json="ewms_workflow__TPJ_ewms__TR_0060__FP_0.01__DTRP_n__WSF_None.json"
        ;;
    C)
        classical="classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_y__WSF_None"
        ewms_json="ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_y__WSF_None.json"
        ;;
    D)
        classical="classical_dag__TPJ_0100__TR_0060__FP_0.00__DTRP_n__WSF_1.0_5.0"
        ewms_json="ewms_workflow__TPJ_ewms__TR_0060__FP_0.00__DTRP_n__WSF_1.0_5.0.json"
        ;;
    *)
        echo "Invalid argument: $choice" >&2
        echo "Valid options are: A, B, C, D" >&2
        exit 1
        ;;
esac

run_pair() {
    local classical_dir="$1"
    local ewms_json="$2"

    echo "Running classical: $classical_dir"
    cd "$classical_dir"
    if [[ -f "${classical_dir}.dag.condor.sub" ]]; then
        echo "WARNING: DAG has already been submitted, skipping this pair."
        return
    fi
    condor_submit_dag "${classical_dir}.dag"
    cd ..

    echo "Running EWMS: $ewms_json"
    local img="/cvmfs/icecube.opensciencegrid.org/containers/ewms/observation-management-service/ewms-condor-benchmarking:main-$BENCHMARK_TAG"
    apptainer run --pwd /app \
        --mount type=bind,source="$(dirname "$img")",dst="$(dirname "$img")",ro \
        --mount type=bind,source="${SCRATCH_DIR%/}",dst="${SCRATCH_DIR%/}" \
        "$img" python ewms_external.py \
        --request-json "$PWD/$ewms_json" \
        --n-tasks 200_000
}

run_pair "$classical" "$ewms_json"
