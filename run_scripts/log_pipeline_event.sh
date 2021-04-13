#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            CPU_PROFILE_OUTPUT_PATH="$2"

            CPU_PROFILE_ARG="--profile-cpu $CPU_PROFILE_OUTPUT_PATH"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 5 ]]; then
    echo "Usage: ./log_pipeline_event.sh [--profile-cpu <cpu-profile-output-path>] <user> <google-cloud-credentials-file-path>\
           <pipeline-configuration-file-path> <run-id> <event-key>"
    echo "Updates pipeline event/status to a firebase table to aid in monitoring"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
EVENT_KEY=$5

cd ..
./docker-run-log-pipeline-event.sh ${CPU_PROFILE_ARG} \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" "$RUN_ID" "$EVENT_KEY"
