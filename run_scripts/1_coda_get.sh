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

    "CSAP_age"
    "CSAP_gender"
    "CSAP_location"
    "CSAP_recently_displaced"
    "CSAP_livelihood"
    "CSAP_household_language"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "e895887b3abceb63bab672a262d5c1dd73dcad92"  # (master which supports incremental get)

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
