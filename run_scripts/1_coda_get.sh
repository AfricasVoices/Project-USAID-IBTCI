#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

DATASETS=(
    "USAID_IBTCI_rqa_s08e01"
    "USAID_IBTCI_rqa_s08e02"
    "USAID_IBTCI_rqa_s08e03"
    "USAID_IBTCI_rqa_s08e03_break"
    "USAID_IBTCI_rqa_s08e04"
    "USAID_IBTCI_rqa_s08e05"
    "USAID_IBTCI_rqa_s08e06"

    "USAID_IBTCI_facebook_s08e01"
    "USAID_IBTCI_facebook_s08e02"
    "USAID_IBTCI_facebook_s08e03"
    "USAID_IBTCI_facebook_s08e03_break_w01"
    "USAID_IBTCI_facebook_s08e03_break_w02"
    "USAID_IBTCI_facebook_s08e03_break_w03"
    "USAID_IBTCI_facebook_s08e03_break_w04"
    "USAID_IBTCI_facebook_s08e03_break_w05"
    "USAID_IBTCI_facebook_s08e03_break_w06"
    "USAID_IBTCI_facebook_s08e04"
    "USAID_IBTCI_facebook_s08e05"
    "USAID_IBTCI_facebook_s08e06"
    "USAID_IBTCI_facebook_s08e06_break_w01"
    "USAID_IBTCI_facebook_s08e06_break_w02"
    "USAID_IBTCI_facebook_s08e06_break_w03"

    "CSAP_age"
    "CSAP_gender"
    "CSAP_location"
    "CSAP_recently_displaced"
    "CSAP_livelihood"
    "CSAP_household_language"

    "USAID_IBTCI_s08_have_voice"
    "USAID_IBTCI_s08_suggestions"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "c47977d03f96ba3e97c704c967c755f0f8b666cb"  # (master which supports incremental add)

mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    FILE="$DATA_ROOT/Coded Coda Files/$DATASET.json"

    if [ -e "$FILE" ]; then
        echo "Getting messages data from ${DATASET} (incremental update)..."
        MESSAGES=$(pipenv run python get.py --previous-export-file-path "$FILE" "$AUTH" "${DATASET}" messages)
        echo "$MESSAGES" >"$FILE"
    else
        echo "Getting messages data from ${DATASET} (full download)..."
        pipenv run python get.py "$AUTH" "${DATASET}" messages >"$FILE"
    fi

done
