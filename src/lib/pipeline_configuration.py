import json
from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urlparse

import pytz
from core_data_modules.data_models import validators
from dateutil.parser import isoparse

from configuration import coding_plans


class PipelineConfiguration(object):
    RQA_CODING_PLANS = []
    DEMOG_CODING_PLANS = []
    FOLLOW_UP_CODING_PLANS = []
    SURVEY_CODING_PLANS = []
    ENGAGEMENT_CODING_PLANS = []
    WS_CORRECT_DATASET_SCHEME = None

    def __init__(self, pipeline_name, raw_data_sources, uuid_table, operations_dashboard, timestamp_remappings,
                 source_key_remappings, project_start_date, project_end_date, filter_test_messages, move_ws_messages,
                 memory_profile_upload_bucket, data_archive_upload_bucket, bucket_dir_path,
                 automated_analysis, drive_upload=None):
        """
        :param pipeline_name: The name of this pipeline.
        :type pipeline_name: str
        :param raw_data_sources: List of sources to pull the various raw run files from.
        :type raw_data_sources: list of RawDataSource
        :param uuid_table: Configuration for the Firestore source id <-> avf uuid table.
        :type uuid_table: UuidTable
        :param operations_dashboard: Configuration for the avf operations dashboard.
        :type operations_dashboard: OperationsDashboard
        :param source_key_remappings: List of source_key -> pipeline_key remappings.
        :type source_key_remappings: list of SourceKeyRemapping
        :param project_start_date: When data collection started - all activation messages received before this date
                                   time will be dropped.
        :type project_start_date: datetime.datetime
        :param project_end_date: When data collection stopped - all activation messages received on or after this date
                                 time will be dropped.
        :type project_end_date: datetime.datetime
        :param filter_test_messages: Whether to filter out messages sent from the rapid_pro_test_contact_uuids
        :type filter_test_messages: bool
        :param move_ws_messages: Whether to move messages labelled as Wrong Scheme to the correct dataset.
        :type move_ws_messages: bool
        :param memory_profile_upload_bucket: The GS bucket name to upload the memory profile log to.
                                              This name will be appended with the log_dir_path
                                              and the file basename to generate the log upload location.
        :type memory_profile_upload_bucket: str
        :param data_archive_upload_bucket: The GS bucket name to upload the data archive file to.
                                            This name will be appended with the log_dir_path
                                            and the file basename to generate the archive upload location.
        :type data_archive_upload_bucket: str
        :param bucket_dir_path: The GS bucket folder path to store the data archive & memory log files to.
        :type bucket_dir_path: str
        :param bucket_dir_path: The GS bucket folder path to store the data archive & memory log files to.
        :type bucket_dir_path: str
        :param automated_analysis: Different Automated analysis Script Configurations
        :type automated_analysis: AutomatedAnalysis
        """
        self.pipeline_name = pipeline_name
        self.raw_data_sources = raw_data_sources
        self.uuid_table = uuid_table
        self.operations_dashboard = operations_dashboard
        self.timestamp_remappings = timestamp_remappings
        self.source_key_remappings = source_key_remappings
        self.project_start_date = project_start_date
        self.project_end_date = project_end_date
        self.filter_test_messages = filter_test_messages
        self.move_ws_messages = move_ws_messages
        self.drive_upload = drive_upload
        self.memory_profile_upload_bucket = memory_profile_upload_bucket
        self.data_archive_upload_bucket = data_archive_upload_bucket
        self.automated_analysis = automated_analysis
        self.bucket_dir_path = bucket_dir_path

        PipelineConfiguration.RQA_CODING_PLANS = coding_plans.get_rqa_coding_plans(self.pipeline_name)
        PipelineConfiguration.DEMOG_CODING_PLANS = coding_plans.get_demog_coding_plans(self.pipeline_name)
        PipelineConfiguration.FOLLOW_UP_CODING_PLANS = coding_plans.get_follow_up_coding_plans(self.pipeline_name)
        PipelineConfiguration.ENGAGEMENT_CODING_PLANS = coding_plans.get_engagement_coding_plans(self.pipeline_name)
        PipelineConfiguration.SURVEY_CODING_PLANS += PipelineConfiguration.DEMOG_CODING_PLANS
        PipelineConfiguration.SURVEY_CODING_PLANS += PipelineConfiguration.FOLLOW_UP_CODING_PLANS
        PipelineConfiguration.SURVEY_CODING_PLANS += PipelineConfiguration.ENGAGEMENT_CODING_PLANS
        PipelineConfiguration.WS_CORRECT_DATASET_SCHEME = coding_plans.get_ws_correct_dataset_scheme(self.pipeline_name)

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        pipeline_name = configuration_dict["PipelineName"]

        raw_data_sources = []
        for raw_data_source in configuration_dict["RawDataSources"]:
            if raw_data_source["SourceType"] == "RapidPro":
                raw_data_sources.append(RapidProSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "GCloudBucket":
                raw_data_sources.append(GCloudBucketSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "RecoveryCSV":
                raw_data_sources.append(RecoveryCSVSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "Facebook":
                raw_data_sources.append(FacebookSource.from_configuration_dict(raw_data_source))
            else:
                assert False, f"Unknown SourceType '{raw_data_source['SourceType']}'. " \
                              f"Must be 'RapidPro', 'GCloudBucket', 'RecoveryCSV', or 'Facebook'."

        uuid_table = UuidTable.from_configuration_dict(
            configuration_dict["UuidTable"])

        operations_dashboard = OperationsDashboard.from_configuration_dict(
            configuration_dict["OperationsDashboard"])

        timestamp_remappings = []
        for remapping_dict in configuration_dict.get("TimestampRemappings", []):
            timestamp_remappings.append(TimestampRemapping.from_configuration_dict(remapping_dict))

        source_key_remappings = []
        for remapping_dict in configuration_dict["SourceKeyRemappings"]:
            source_key_remappings.append(SourceKeyRemapping.from_configuration_dict(remapping_dict))

        project_start_date = isoparse(configuration_dict["ProjectStartDate"])
        project_end_date = isoparse(configuration_dict["ProjectEndDate"])

        filter_test_messages = configuration_dict["FilterTestMessages"]
        move_ws_messages = configuration_dict["MoveWSMessages"]

        automated_analysis = AutomatedAnalysis.from_configuration_dict(configuration_dict["AutomatedAnalysis"])

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        memory_profile_upload_bucket = configuration_dict["MemoryProfileUploadBucket"]
        data_archive_upload_bucket = configuration_dict["DataArchiveUploadBucket"]
        bucket_dir_path = configuration_dict["BucketDirPath"]

        return cls(pipeline_name, raw_data_sources, uuid_table, operations_dashboard, timestamp_remappings,
                   source_key_remappings, project_start_date, project_end_date, filter_test_messages,
                   move_ws_messages, memory_profile_upload_bucket, data_archive_upload_bucket, bucket_dir_path,
                   automated_analysis, drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))

    def validate(self):
        validators.validate_string(self.pipeline_name, "pipeline_name")

        validators.validate_list(self.raw_data_sources, "raw_data_sources")
        for i, raw_data_source in enumerate(self.raw_data_sources):
            assert isinstance(raw_data_source, RawDataSource), f"raw_data_sources[{i}] is not of type RawDataSource"
            raw_data_source.validate()

        assert isinstance(self.uuid_table, UuidTable)
        self.uuid_table.validate()

        assert isinstance(self.operations_dashboard, OperationsDashboard)
        self.operations_dashboard.validate()

        validators.validate_list(self.source_key_remappings, "source_key_remappings")
        for i, remapping in enumerate(self.source_key_remappings):
            assert isinstance(remapping, SourceKeyRemapping), \
                f"source_key_mappings[{i}] is not of type SourceKeyRemapping"
            remapping.validate()

        validators.validate_datetime(self.project_start_date, "project_start_date")
        validators.validate_datetime(self.project_end_date, "project_end_date")

        validators.validate_bool(self.filter_test_messages, "filter_test_messages")
        validators.validate_bool(self.move_ws_messages, "move_ws_messages")

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()

        validators.validate_url(self.memory_profile_upload_bucket, "memory_profile_upload_bucket", "gs")
        validators.validate_url(self.data_archive_upload_bucket, "data_archive_upload_bucket", "gs")
        validators.validate_string(self.bucket_dir_path, "bucket_dir_path")


class RawDataSource(ABC):
    @abstractmethod
    def get_activation_flow_names(self):
        pass

    @abstractmethod
    def get_survey_flow_names(self):
        pass

    @abstractmethod
    def validate(self):
        pass


class RapidProSource(RawDataSource):
    def __init__(self, domain, token_file_url, activation_flow_names, survey_flow_names, test_contact_uuids):
        """
        :param domain: URL of the Rapid Pro server to download data from.
        :type domain: str
        :param token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro server.
        :type token_file_url: str
        :param activation_flow_names: The names of the RapidPro flows that contain the radio show responses.
        :type: activation_flow_names: list of str
        :param survey_flow_names: The names of the RapidPro flows that contain the survey responses.
        :type: survey_flow_names: list of str
        :param test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                   Runs for any of those test contacts will be tagged with {'test_run': True},
                                   and dropped when the pipeline is run with "FilterTestMessages" set to true.
        :type test_contact_uuids: list of str
        """
        self.domain = domain
        self.token_file_url = token_file_url
        self.activation_flow_names = activation_flow_names
        self.survey_flow_names = survey_flow_names
        self.test_contact_uuids = test_contact_uuids

        self.validate()

    def get_activation_flow_names(self):
        return self.activation_flow_names

    def get_survey_flow_names(self):
        return self.survey_flow_names

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        domain = configuration_dict["Domain"]
        token_file_url = configuration_dict["TokenFileURL"]
        activation_flow_names = configuration_dict.get("ActivationFlowNames", [])
        survey_flow_names = configuration_dict.get("SurveyFlowNames", [])
        test_contact_uuids = configuration_dict.get("TestContactUUIDs", [])

        return cls(domain, token_file_url, activation_flow_names, survey_flow_names, test_contact_uuids)

    def validate(self):
        validators.validate_string(self.domain, "domain")
        validators.validate_string(self.token_file_url, "token_file_url")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.survey_flow_names, "survey_flow_names")
        for i, survey_flow_name in enumerate(self.survey_flow_names):
            validators.validate_string(survey_flow_name, f"survey_flow_names[{i}]")

        validators.validate_list(self.test_contact_uuids, "test_contact_uuids")
        for i, contact_uuid in enumerate(self.test_contact_uuids):
            validators.validate_string(contact_uuid, f"test_contact_uuids[{i}]")


class AbstractRemoteURLSource(RawDataSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        self.activation_flow_urls = activation_flow_urls
        self.survey_flow_urls = survey_flow_urls

        self.validate()

    def get_activation_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.activation_flow_urls]

    def get_survey_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.survey_flow_urls]

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        activation_flow_urls = configuration_dict.get("ActivationFlowURLs", [])
        survey_flow_urls = configuration_dict.get("SurveyFlowURLs", [])

        return cls(activation_flow_urls, survey_flow_urls)

    def validate(self):
        validators.validate_list(self.activation_flow_urls, "activation_flow_urls")
        for i, activation_flow_url in enumerate(self.activation_flow_urls):
            validators.validate_url(activation_flow_url, f"activation_flow_urls[{i}]", "gs")

        validators.validate_list(self.survey_flow_urls, "survey_flow_urls")
        for i, survey_flow_url in enumerate(self.survey_flow_urls):
            validators.validate_url(survey_flow_url, f"survey_flow_urls[{i}]", "gs")


class GCloudBucketSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class RecoveryCSVSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class FacebookSource(RawDataSource):
    def __init__(self, page_id, token_file_url, datasets):
        self.page_id = page_id
        self.token_file_url = token_file_url
        self.datasets = datasets

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        page_id = configuration_dict["PageID"]
        token_file_url = configuration_dict["TokenFileURL"]
        datasets = [FacebookDataset.from_configuration_dict(d) for d in configuration_dict["Datasets"]]

        return cls(page_id, token_file_url, datasets)

    def validate(self):
        validators.validate_string(self.page_id, "page_id")
        validators.validate_url(self.token_file_url, "token_file_url", scheme="gs")

        validators.validate_list(self.datasets, "datasets")
        for i, dataset in enumerate(self.datasets):
            assert isinstance(dataset, FacebookDataset), f"datasets[{i}] is not of type FacebookDataset"
            dataset.validate()

    # TODO: Rename to refer to datasets instead of flows, since 'flows' don't really make sense for Facebook
    def get_activation_flow_names(self):
        return [f"{dataset.name}_{self.page_id}" for dataset in self.datasets]

    def get_survey_flow_names(self):
        return []


class FacebookDataset(object):
    def __init__(self, name, post_ids=None, search=None):
        self.name = name
        self.post_ids = post_ids
        self.search = search

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        name = configuration_dict["Name"]
        post_ids = configuration_dict.get("PostIDs")
        search = configuration_dict.get("Search")

        if search is not None:
            search = FacebookSearch.from_configuration_dict(search)

        return cls(name, post_ids, search)

    def validate(self):
        validators.validate_string(self.name, "name")

        assert self.post_ids is not None or self.search is not None, \
            "Must provide at least one of post_id or search"

        if self.post_ids is not None:
            validators.validate_list(self.post_ids, "post_ids")
            for i, post_id in enumerate(self.post_ids):
                validators.validate_string(post_id, f"post_ids[{i}]")

        if self.search is not None:
            self.search.validate()


class FacebookSearch(object):
    def __init__(self, match, start_date, end_date):
        self.match = match
        self.start_date = start_date
        self.end_date = end_date

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        match = configuration_dict["Match"]
        start_date = isoparse(configuration_dict["StartDate"])
        end_date = isoparse(configuration_dict["EndDate"])

        return cls(match, start_date, end_date)

    def validate(self):
        validators.validate_string(self.match, "match")
        validators.validate_datetime(self.start_date, "start_date")
        validators.validate_datetime(self.end_date, "end_date")


class UuidTable(object):
    def __init__(self, firebase_credentials_file_url, table_name, uuid_prefix):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                              the source id <-> avf uuid table is stored.
        :type firebase_credentials_file_url: str
        :param table_name: Name of the data <-> uuid table in Firebase to use.
        :type table_name: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url
        self.table_name = table_name
        self.uuid_prefix = uuid_prefix

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]
        table_name = configuration_dict["TableName"]
        uuid_prefix = configuration_dict["UuidPrefix"]

        return cls(firebase_credentials_file_url, table_name, uuid_prefix)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")
        validators.validate_string(self.table_name, "table_name")
        validators.validate_string(self.uuid_prefix, "uuid_prefix")


class TimestampRemapping(object):
    def __init__(self, time_key, show_pipeline_key_to_remap_to, range_start_inclusive=None, range_end_exclusive=None,
                 time_to_adjust_to=None):
        """
        Specifies a remapping of messages received within the given time range to another radio show field.
        Optionally specifies an adjustment of all affected timestamps to a constant datetime.

        :param time_key: Key in each TracedData of an ISO 8601-formatted datetime string to read the message sent on
                         time from.
        :type time_key: str
        :param show_pipeline_key_to_remap_to: Pipeline key to assign to messages received within the given time range.
        :type show_pipeline_key_to_remap_to: str
        :param range_start_inclusive: Start datetime for the time range to remap radio show messages from, inclusive.
                                      If None, defaults to the beginning of time.
        :type range_start_inclusive: datetime | None
        :param range_end_exclusive: End datetime for the time range to remap radio show messages from, exclusive.
                                    If None, defaults to the end of time.
        :type range_end_exclusive: datetime | None
        :param time_to_adjust_to: Datetime to adjust each message object's `time_key` field to, or None.
                                  If None, re-mapped shows will not have timestamps adjusted.
        :type time_to_adjust_to: datetime | None
        """
        if range_start_inclusive is None:
            range_start_inclusive = pytz.utc.localize(datetime.min)
        if range_end_exclusive is None:
            range_end_exclusive = pytz.utc.localize(datetime.max)

        self.time_key = time_key
        self.show_pipeline_key_to_remap_to = show_pipeline_key_to_remap_to
        self.range_start_inclusive = range_start_inclusive
        self.range_end_exclusive = range_end_exclusive
        self.time_to_adjust_to = time_to_adjust_to

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        time_key = configuration_dict["TimeKey"]
        show_pipeline_key_to_remap_to = configuration_dict["ShowPipelineKeyToRemapTo"]
        range_start_inclusive = configuration_dict.get("RangeStartInclusive")
        range_end_exclusive = configuration_dict.get("RangeEndExclusive")
        time_to_adjust_to = configuration_dict.get("TimeToAdjustTo")

        if range_start_inclusive is not None:
            range_start_inclusive = isoparse(range_start_inclusive)
        if range_end_exclusive is not None:
            range_end_exclusive = isoparse(range_end_exclusive)
        if time_to_adjust_to is not None:
            time_to_adjust_to = isoparse(time_to_adjust_to)

        return cls(time_key, show_pipeline_key_to_remap_to, range_start_inclusive, range_end_exclusive,
                   time_to_adjust_to)

    def validate(self):
        validators.validate_string(self.time_key, "time_key")
        validators.validate_string(self.show_pipeline_key_to_remap_to, "show_pipeline_key_to_remap_to")
        validators.validate_datetime(self.range_start_inclusive, "range_start_inclusive")
        validators.validate_datetime(self.range_end_exclusive, "range_end_exclusive")

        if self.time_to_adjust_to is not None:
            validators.validate_datetime(self.time_to_adjust_to, "time_to_adjust_to")


class SourceKeyRemapping(object):
    def __init__(self, is_activation_message, source_key, pipeline_key):
        """
        :param is_activation_message: Whether this re-mapping contains an activation message (activation messages need
                                   to be handled differently because they are not always in the correct dataset)
        :type is_activation_message: bool
        :param source_key: Name of key in the dataset exported by fetch_raw_data.py.
        :type source_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.is_activation_message = is_activation_message
        self.source_key = source_key
        self.pipeline_key = pipeline_key

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        is_activation_message = configuration_dict.get("IsActivationMessage", False)
        source_key = configuration_dict["SourceKey"]
        pipeline_key = configuration_dict["PipelineKey"]

        return cls(is_activation_message, source_key, pipeline_key)

    def validate(self):
        validators.validate_bool(self.is_activation_message, "is_activation_message")
        validators.validate_string(self.source_key, "source_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, automated_analysis_dir):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param automated_analysis_dir: Directory in the Drive service account's "Shared with Me" directory to upload the
                                    automated analysis files from this pipeline run to.
        :type automated_analysis_dir: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.automated_analysis_dir = automated_analysis_dir

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        automated_analysis_dir = configuration_dict["AutomatedAnalysisDir"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, automated_analysis_dir)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.automated_analysis_dir, "automated_analysis_dir")


class AutomatedAnalysis(object):
    def __init__(self, generate_region_theme_distribution_maps, generate_district_theme_distribution_maps,
                 generate_mogadishu_theme_distribution_maps, traffic_labels=None):
        """
        :param generate_region_theme_distribution_maps: Whether to generate somali region theme distribution maps.
        :type generate_region_theme_distribution_maps: bool
        :param generate_district_theme_distribution_maps: Whether to generate somali district theme distribution maps.
        :type generate_district_theme_distribution_maps: bool
        :param generate_mogadishu_theme_distribution_maps: Whether to generate mogadishu sub-district theme distribution maps.
        :type generate_mogadishu_theme_distribution_maps: bool
        """
        self.generate_region_theme_distribution_maps = generate_region_theme_distribution_maps
        self.generate_district_theme_distribution_maps = generate_district_theme_distribution_maps
        self.generate_mogadishu_theme_distribution_maps = generate_mogadishu_theme_distribution_maps
        self.traffic_labels = traffic_labels

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        generate_region_theme_distribution_maps = configuration_dict["GenerateRegionThemeDistributionMaps"]
        generate_district_theme_distribution_maps = configuration_dict["GenerateDistrictThemeDistributionMaps"]
        generate_mogadishu_theme_distribution_maps = configuration_dict["GenerateMogadishuThemeDistributionMaps"]
        traffic_labels = configuration_dict.get("TrafficLabels")
        if traffic_labels is not None:
            traffic_labels = [TrafficLabel.from_configuration_dict(d) for d in traffic_labels]

        return cls(generate_region_theme_distribution_maps, generate_district_theme_distribution_maps,
                   generate_mogadishu_theme_distribution_maps, traffic_labels)

    def validate(self):
        validators.validate_bool(self.generate_region_theme_distribution_maps,
                                 "generate_region_theme_distribution_maps")
        validators.validate_bool(self.generate_district_theme_distribution_maps,
                                 "generate_district_theme_distribution_maps")
        validators.validate_bool(self.generate_mogadishu_theme_distribution_maps,
                                 "generate_mogadishu_theme_distribution_maps")

        if self.traffic_labels is not None:
            assert isinstance(self.traffic_labels, list)
            for tl in self.traffic_labels:
                assert isinstance(tl, TrafficLabel)
                tl.validate()


class OperationsDashboard(object):
    def __init__(self, firebase_credentials_file_url):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                                 the Operations Dashboard data is stored.
        :type firebase_credentials_file_url: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]

        return cls(firebase_credentials_file_url)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")


class TrafficLabel(object):
    def __init__(self, label, start_date, end_date):
        self.label = label
        self.start_date = start_date
        self.end_date = end_date

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        label = configuration_dict["Label"]
        start_date = isoparse(configuration_dict["StartDate"])
        end_date = isoparse(configuration_dict["EndDate"])

        return cls(label, start_date, end_date)

    def validate(self):
        validators.validate_string(self.label, "label")
        validators.validate_datetime(self.start_date, "start_date")
        validators.validate_datetime(self.end_date, "end_date")
