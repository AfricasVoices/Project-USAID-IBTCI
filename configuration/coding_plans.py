from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies

from configuration import code_imputation_functions
from configuration.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingConfiguration, CodingModes, CodingPlan


def clean_age_with_range_filter(text):
    """
    Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
    """
    age = swahili.DemographicCleaner.clean_age(text)
    if type(age) == int and 10 <= age < 100:
        return str(age)
        # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
        #       NC in the case where age is an int but is out of range
    else:
        return Codes.NOT_CODED


def clean_district_if_no_mogadishu_sub_district(text):
    mogadishu_sub_district = somali.DemographicCleaner.clean_mogadishu_sub_district(text)
    if mogadishu_sub_district == Codes.NOT_CODED:
        return somali.DemographicCleaner.clean_somalia_district(text)
    else:
        return Codes.NOT_CODED


def get_rqa_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return [
            CodingPlan(raw_field="facebook_s01e01_raw",
                       time_field="sent_on",
                       run_id_field="facebook_s01e01_run_id",
                       coda_filename="USAID_IBTCI_facebook_s01e01.json",
                       icr_filename="facebook_s01e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.FACEBOOK_S01E01,
                               coded_field="facebook_s01e01_coded",
                               analysis_file_key="facebook_s01e01",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.FACEBOOK_S01E01, x, y)
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate),
            CodingPlan(raw_field="facebook_s01e02_raw",
                       time_field="sent_on",
                       run_id_field="facebook_s01e02_run_id",
                       coda_filename="USAID_IBTCI_facebook_s01e02.json",
                       icr_filename="facebook_s01e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.FACEBOOK_S01E02,
                               coded_field="facebook_s01e02_coded",
                               analysis_file_key="facebook_s01e02",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.FACEBOOK_S01E02, x, y)
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]
    else:
        return []


def get_demog_coding_plans(pipeline_name):
    return []


def get_follow_up_coding_plans(pipeline_name):
    return []


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET
