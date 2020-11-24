from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import SHAUtils

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


def clean_facebook_post_type(post):
    post_type = None
    # Assume that there is only one attachment type, which is either a photo or inline_video, as this is the plan for
    # this project. If that assumption doesn't hold, this will fail and we can adapt to what the data actually looks
    # in that case when we see it.
    for attachment in post["attachments"]["data"]:
        assert attachment["type"] in {"video_inline", "photo"}, post

        if attachment["type"] == "video_inline":
            assert post_type in {"video", None}, post
            post_type = "video"
        elif attachment["type"] == "photo":
            assert post_type in {"photo", None}, post
            post_type = "photo"

    return post_type


def get_rqa_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return [
            CodingPlan(raw_field="facebook_s08e01_raw",
                       time_field="sent_on",
                       run_id_field="facebook_s08e01_run_id",
                       coda_filename="USAID_IBTCI_facebook_s08e01.json",
                       message_id_fn=lambda td: SHAUtils.sha_string(td["facebook_s08e01_comment_id"]),
                       icr_filename="facebook_s08e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.FACEBOOK_S08E01,
                               coded_field="facebook_s08e01_coded",
                               analysis_file_key="facebook_s08e01",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.FACEBOOK_S08E01, x, y)
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e01_comment_reply_to_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_COMMENT_REPLY_TO,
                               cleaner=lambda parent: "post" if parent == {} else "comment",
                               coded_field="facebook_s08e01_comment_reply_to_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e01_comment_reply_to",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e01_post_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_POST_TYPE,
                               cleaner=clean_facebook_post_type,
                               coded_field="facebook_s08e01_post_type_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e01_post_type",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="facebook_s08e02_raw",
                       time_field="sent_on",
                       run_id_field="facebook_s08e02_run_id",
                       coda_filename="USAID_IBTCI_facebook_s08e02.json",
                       message_id_fn=lambda td: SHAUtils.sha_string(td["facebook_s08e02_comment_id"]),
                       icr_filename="facebook_s08e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.FACEBOOK_S08E02,
                               coded_field="facebook_s08e02_coded",
                               analysis_file_key="facebook_s08e02",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.FACEBOOK_S08E02, x,
                                                                                        y)
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e02_comment_reply_to_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_COMMENT_REPLY_TO,
                               cleaner=lambda parent: "post" if parent == {} else "comment",
                               coded_field="facebook_s08e02_comment_reply_to_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e02_comment_reply_to",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e02_post_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_POST_TYPE,
                               cleaner=clean_facebook_post_type,
                               coded_field="facebook_s08e02_post_type_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e02_post_type",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="facebook_s08e03_raw",
                       time_field="sent_on",
                       run_id_field="facebook_s08e03_run_id",
                       coda_filename="USAID_IBTCI_facebook_s08e03.json",
                       message_id_fn=lambda td: SHAUtils.sha_string(td["facebook_s08e03_comment_id"]),
                       icr_filename="facebook_s08e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.FACEBOOK_S08E03,
                               coded_field="facebook_s08e03_coded",
                               analysis_file_key="facebook_s08e03",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.FACEBOOK_S08E03, x,
                                                                                        y)
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e03_comment_reply_to_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_COMMENT_REPLY_TO,
                               cleaner=lambda parent: "post" if parent == {} else "comment",
                               coded_field="facebook_s08e03_comment_reply_to_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e03_comment_reply_to",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           ),
                           CodingConfiguration(
                               raw_field="facebook_s08e03_post_raw",
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.FACEBOOK_POST_TYPE,
                               cleaner=clean_facebook_post_type,
                               coded_field="facebook_s08e03_post_type_coded",
                               requires_manual_verification=False,
                               analysis_file_key="facebook_s08e03_post_type",
                               fold_strategy=None,
                               include_in_individuals_file=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]
    else:
        assert pipeline_name == "USAID-IBTCI-SMS"
        return [
            CodingPlan(raw_field="rqa_s08e01_raw",
                       time_field="sent_on",
                       run_id_field="rqa_s08e01_run_id",
                       coda_filename="USAID_IBTCI_rqa_s08e01.json",
                       icr_filename="rqa_s08e01.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.RQA_S08E01,
                               coded_field="rqa_s08e01_coded",
                               analysis_file_key="rqa_s08e01",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.RQA_S08E01, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s08e01"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s08e02_raw",
                       time_field="sent_on",
                       run_id_field="rqa_s08e02_run_id",
                       coda_filename="USAID_IBTCI_rqa_s08e02.json",
                       icr_filename="rqa_s08e02.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.RQA_S08E02,
                               coded_field="rqa_s08e02_coded",
                               analysis_file_key="rqa_s08e02",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.RQA_S08E02, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s08e02"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s08e03_raw",
                       time_field="sent_on",
                       run_id_field="rqa_s08e03_run_id",
                       coda_filename="USAID_IBTCI_rqa_s08e03.json",
                       icr_filename="rqa_s08e03.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.RQA_S08E03,
                               coded_field="rqa_s08e03_coded",
                               analysis_file_key="rqa_s08e03",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.RQA_S08E03, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s08e03"),
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(raw_field="rqa_s08e03_break_raw",
                       time_field="sent_on",
                       run_id_field="rqa_s08e03_break_run_id",
                       coda_filename="USAID_IBTCI_rqa_s08e03_break.json",
                       icr_filename="rqa_s08e03_break.csv",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.MULTIPLE,
                               code_scheme=CodeSchemes.RQA_S08E03_BREAK,
                               coded_field="rqa_s08e03_break_coded",
                               analysis_file_key="rqa_s08e03_break",
                               fold_strategy=lambda x, y: FoldStrategies.list_of_labels(CodeSchemes.RQA_S08E03_BREAK, x, y)
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s08e03_break"),
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]


def get_demog_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return []
    else:
        assert pipeline_name == "USAID-IBTCI-SMS"
        return [
            CodingPlan(raw_field="operator_raw",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.SOMALIA_OPERATOR,
                               coded_field="operator_coded",
                               analysis_file_key="operator",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="location_raw",
                       time_field="location_time",
                       coda_filename="CSAP_location.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT,
                               cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                               coded_field="mogadishu_sub_district_coded",
                               analysis_file_key="mogadishu_sub_district",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           ),
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.SOMALIA_DISTRICT,
                               cleaner=clean_district_if_no_mogadishu_sub_district,
                               coded_field="district_coded",
                               analysis_file_key="district",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           ),
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.SOMALIA_REGION,
                               coded_field="region_coded",
                               analysis_file_key="region",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           ),
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.SOMALIA_STATE,
                               coded_field="state_coded",
                               analysis_file_key="state",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           ),
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.SOMALIA_ZONE,
                               coded_field="zone_coded",
                               analysis_file_key="zone",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       code_imputation_function=code_imputation_functions.impute_somalia_location_codes,
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="gender_raw",
                       time_field="gender_time",
                       coda_filename="CSAP_gender.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.GENDER,
                               cleaner=somali.DemographicCleaner.clean_gender,
                               coded_field="gender_coded",
                               analysis_file_key="gender",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("gender"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="age_raw",
                       time_field="age_time",
                       coda_filename="CSAP_age.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.AGE,
                               cleaner=clean_age_with_range_filter,
                               coded_field="age_coded",
                               analysis_file_key="age",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                           # CodingConfiguration(
                           #     coding_mode=CodingModes.SINGLE,
                           #     code_scheme=CodeSchemes.AGE_CATEGORY,
                           #     coded_field="age_category_coded",
                           #     analysis_file_key="age_category",
                           #     fold_strategy=FoldStrategies.assert_label_ids_equal
                           # )
                       ],
                       # code_imputation_function=code_imputation_functions.impute_age_category,
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="recently_displaced_raw",
                       time_field="recently_displaced_time",
                       coda_filename="CSAP_recently_displaced.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.RECENTLY_DISPLACED,
                               cleaner=somali.DemographicCleaner.clean_yes_no,
                               coded_field="recently_displaced_coded",
                               analysis_file_key="recently_displaced",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("recently displaced"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="livelihood_raw",
                       time_field="livelihood_time",
                       coda_filename="CSAP_livelihood.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.LIVELIHOOD,
                               coded_field="livelihood_coded",
                               analysis_file_key="livelihood",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("livelihood"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

            CodingPlan(raw_field="household_language_raw",
                       time_field="household_language_time",
                       coda_filename="CSAP_household_language.json",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.HOUSEHOLD_LANGUAGE,
                               cleaner=None,
                               coded_field="household_language_coded",
                               analysis_file_key="household_language",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("household language"),
                       raw_field_fold_strategy=FoldStrategies.assert_equal)
        ]


def get_follow_up_coding_plans(pipeline_name):
    return []


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET
