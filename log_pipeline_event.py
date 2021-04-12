import argparse
import json

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from storage.google_cloud import google_cloud_utils
from pipeline_logs.firestore_pipeline_logger import FirestorePipelineLogger

from src.lib import PipelineConfiguration
from src.lib.configuration_objects import PipelineEvents

log = Logger(__name__)

def log_pipeline_event(user, google_cloud_credentials_file_path, pipeline_configuration_file_path, run_id, event_key):
    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info("Downloading Firestore Operations Dashboard Table credentials...")
    firestore_pipeline_logs_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.operations_dashboard.firebase_credentials_file_url
    ))

    log.info(f"Writing {event_key} event log for run_id: {run_id}")
    firestore_pipeline_logger = FirestorePipelineLogger(pipeline_configuration.pipeline_name, run_id,
                                                        firestore_pipeline_logs_table_credentials)

    firestore_pipeline_logger.log_event(TimeUtils.utc_now_as_iso_string(), event_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Updates current pipeline event/stage to a firebase table to aid in monitoring")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("event_key", metavar="event-key",
                        help="Key for this pipeline event/stage",
                        choices=[PipelineEvents.PIPELINE_RUN_START, PipelineEvents.CODA_ADD, 
                                PipelineEvents.FETCHING_RAW_DATA,
                                PipelineEvents.GENERATING_OUTPUTS, PipelineEvents.CODA_GET,
                                PipelineEvents.GENERATING_AUTOMATED_ANALYSIS_FILES, PipelineEvents.BACKING_UP_DATA,
                                PipelineEvents.UPLOADING_ANALYSIS_FILES, PipelineEvents.UPLOADING_LOG_FILES,
                                PipelineEvents.PIPELINE_RUN_END]),

    args = parser.parse_args()

    log_pipeline_event(args.user, args.google_cloud_credentials_file_path, args.pipeline_configuration_file_path,
                        args.run_id, args.event_key)
