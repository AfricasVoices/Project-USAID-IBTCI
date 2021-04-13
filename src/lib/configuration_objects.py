from core_data_modules.util import SHAUtils


class CodingModes(object):
    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class CodingConfiguration(object):
    def __init__(self, coding_mode, code_scheme, coded_field, fold_strategy, raw_field=None,
                 requires_manual_verification=True, analysis_file_key=None, cleaner=None,
                 include_in_theme_distribution=True, include_in_individuals_file=True):
        assert coding_mode in {CodingModes.SINGLE, CodingModes.MULTIPLE}

        self.coding_mode = coding_mode
        self.code_scheme = code_scheme
        self.coded_field = coded_field
        self.raw_field = raw_field
        self.requires_manual_verification = requires_manual_verification
        self.analysis_file_key = analysis_file_key
        self.fold_strategy = fold_strategy
        self.cleaner = cleaner
        self.include_in_theme_distribution = include_in_theme_distribution
        self.include_in_individuals_file = include_in_individuals_file


# TODO: Rename CodingPlan to something like DatasetConfiguration?
class CodingPlan(object):
    def __init__(self, raw_field, coding_configurations, raw_field_fold_strategy, dataset_name=None, coda_filename=None,
                 ws_code=None, time_field=None, run_id_field=None, icr_filename=None, id_field=None,
                 code_imputation_function=None, message_id_fn=None):
        if message_id_fn is None:
            message_id_fn = lambda td: SHAUtils.sha_string(td[self.raw_field])

        if dataset_name is None:
            dataset_name = raw_field

        self.dataset_name = dataset_name
        self.raw_field = raw_field
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.coding_configurations = coding_configurations
        self.code_imputation_function = code_imputation_function
        self.ws_code = ws_code
        self.raw_field_fold_strategy = raw_field_fold_strategy
        self.message_id_fn = message_id_fn

        if id_field is None:
            id_field = f"{self.raw_field}_id"
        self.id_field = id_field


class PipelineEvents(object):
    PIPELINE_RUN_START = "PipelineRunStart"
    CODA_ADD = "CodaAdd"
    FETCHING_RAW_DATA = "FetchingRawData"
    GENERATING_OUTPUTS = "GeneratingOutputs"
    CODA_GET = "CodaGet"
    GENERATING_AUTOMATED_ANALYSIS_FILES = "GeneratingAutomatedAnalysisFiles"
    BACKING_UP_DATA = "BackingUpData"
    UPLOADING_ANALYSIS_FILES = "UploadingAnalysisFiles"
    UPLOADING_LOG_FILES = "UploadingLogFiles"
    PIPELINE_RUN_END = "PipelineRunEnd"
