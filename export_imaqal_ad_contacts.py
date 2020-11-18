import argparse
import csv
import json
import random

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.codes import SomaliaCodes
from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates a sample of phone numbers to advertise to using an "
                                                 "imaqal analysis file")

    parser.add_argument("--exclusion-list-file-path", nargs="?",
                        help="List of phone numbers to exclude from the ad group")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS url to the credentials file to use to access the uuid table")
    parser.add_argument("uuid_table_name", metavar="uuid-table-name",
                        help="Name of the uuid table to use to re-identify the avf ids")
    parser.add_argument("analysis_csv_paths", metavar="analysis-csv-paths", nargs="+",
                        help="Paths to the analysis csv files (either messages or individuals) to extract phone "
                             "numbers from")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to a CSV file to write the contacts from the locations of interest to. "
                             "Exported file is in a format suitable for direct upload to Rapid Pro")

    args = parser.parse_args()

    exclusion_list_file_path = args.exclusion_list_file_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    uuid_table_name = args.uuid_table_name
    analysis_csv_paths = args.analysis_csv_paths
    csv_output_file_path = args.csv_output_file_path

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        uuid_table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    all_uuids = set()
    banadir_uuids = set()
    sws_uuids = set()
    for path in analysis_csv_paths:
        # Load the traced data
        log.info(f"Loading analysis file '{path}'...")
        with open(path) as f:
            data = list(csv.DictReader(f))
        log.info(f"Loaded {len(data)} rows")

        for row in data:
            if row["consent_withdrawn"] == Codes.TRUE:
                continue

            if row["state"] == SomaliaCodes.BANADIR:
                banadir_uuids.add(row["uid"])

            if row["state"] == SomaliaCodes.SOUTH_WEST_STATE:
                sws_uuids.add(row["uid"])

            all_uuids.add(row["uid"])
    log.info(f"Loaded {len(all_uuids)} uuids ({len(banadir_uuids)} banadir, {len(sws_uuids)} sws)")
    banadir_uuids_pre_exclusion_list = banadir_uuids.copy()
    sws_uuids_pre_exclusion_list = sws_uuids.copy()

    if exclusion_list_file_path is not None:
        # Load the exclusion list
        log.info(f"Loading the exclusion list from {exclusion_list_file_path}...")
        with open(exclusion_list_file_path) as f:
            exclusion_list = json.load(f)
        log.info(f"Loaded {len(exclusion_list)} numbers to exclude")

        # Remove any uuids in the exclusion list
        log.info(f"Removing exclusion list uuids from the contacts group")
        removed = 0
        for uuid in exclusion_list:
            if uuid in all_uuids:
                removed += 1
                all_uuids.remove(uuid)

            if uuid in banadir_uuids:
                banadir_uuids.remove(uuid)

            if uuid in sws_uuids:
                sws_uuids.remove(uuid)

        log.info(f"Removed {removed} uuids; {len(all_uuids)} remain ({len(banadir_uuids)} banadir, {len(sws_uuids)} sws)")

    # Take a random sample of 15% of participants from each location
    sampled_banadir = random.sample(banadir_uuids, int(0.15 * len(banadir_uuids_pre_exclusion_list)))
    sampled_sws = random.sample(sws_uuids, int(0.15 * len(sws_uuids_pre_exclusion_list)))
    log.info(f"Sampled {len(sampled_banadir)}/{len(banadir_uuids)} banadir contacts")
    log.info(f"Sampled {len(sampled_sws)}/{len(sws_uuids)} sws contacts")
    uuids = sampled_banadir + sampled_sws

    # Convert the uuids to phone numbers
    log.info(f"Converting {len(uuids)} uuids to phone numbers...")
    uuid_phone_number_lut = phone_number_uuid_table.uuid_to_data_batch(uuids)
    phone_numbers = set()
    for uuid in uuids:
        phone_numbers.add(f"+{uuid_phone_number_lut[uuid]}")
    log.info(f"Successfully converted {len(phone_numbers)} uuids to phone numbers.")

    # Export contacts CSV
    log.warning(f"Exporting {len(phone_numbers)} phone numbers to {csv_output_file_path}...")
    with open(csv_output_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
        writer.writeheader()

        for n in phone_numbers:
            writer.writerow({
                "URN:Tel": n
            })
        log.info(f"Wrote {len(phone_numbers)} contacts to {csv_output_file_path}")
