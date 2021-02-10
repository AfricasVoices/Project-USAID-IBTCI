from core_data_modules.cleaners import somali, swahili, Codes
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import SHAUtils
from dateutil.parser import isoparse
from social_media_tools.facebook import facebook_utils

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


def clean_engagement_type(sent_on, episode):
    time_ranges = [
        ["rqa_s08e01", "sms_ad",      "2020-11-01T16:30+03:00", "2020-11-01T24:00+03:00"],
        ["rqa_s08e01", "radio_promo", "2020-11-02T00:00+03:00", "2020-11-04T24:00+03:00"],
        ["rqa_s08e01", "radio_show",  "2020-11-05T00:00+03:00", "2020-11-05T24:00+03:00"],
        ["rqa_s08e01", "other",       "2020-11-01T00:00+03:00", "2020-11-07T24:00+03:00"],  # by fall-through :S

        ["rqa_s08e02", "sms_ad",      "2020-11-08T16:30+03:00", "2020-11-08T24:00+03:00"],
        ["rqa_s08e02", "radio_promo", "2020-11-09T00:00+03:00", "2020-11-11T24:00+03:00"],
        ["rqa_s08e02", "radio_show",  "2020-11-12T00:00+03:00", "2020-11-12T24:00+03:00"],
        ["rqa_s08e02", "other",       "2020-11-08T00:00+03:00", "2020-11-14T24:00+03:00"],

        ["rqa_s08e03", "sms_ad",      "2020-11-15T16:30+03:00", "2020-11-15T24:00+03:00"],
        ["rqa_s08e03", "sms_ad",      "2020-11-18T16:30+03:00", "2020-11-18T24:00+03:00"],
        ["rqa_s08e03", "sms_ad",      "2020-11-20T16:30+03:00", "2020-11-20T24:00+03:00"],
        ["rqa_s08e03", "radio_promo", "2020-11-16T00:00+03:00", "2020-11-18T24:00+03:00"],  # fall-through
        ["rqa_s08e03", "radio_show",  "2020-11-19T00:00+03:00", "2020-11-19T24:00+03:00"],
        ["rqa_s08e03", "other",       "2020-11-15T00:00+03:00", "2020-11-24T24:00+03:00"],
    ]

    for time_range in time_ranges:
        if episode == time_range[0] and isoparse(time_range[2]) <= sent_on < isoparse(time_range[3]):
            return time_range[1]

    return Codes.TRUE_MISSING


def _make_facebook_coding_plan(name, code_scheme):
    return \
        CodingPlan(dataset_name=f"facebook_{name}",
                   raw_field=f"facebook_{name}_raw",
                   time_field="sent_on",
                   run_id_field=f"facebook_{name}_run_id",
                   coda_filename=f"USAID_IBTCI_facebook_{name}.json",
                   message_id_fn=lambda td: SHAUtils.sha_string(td[f"facebook_{name}_comment_id"]),
                   icr_filename=f"facebook_{name}.csv",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=code_scheme,
                           coded_field=f"facebook_{name}_coded",
                           analysis_file_key=f"facebook_{name}",
                           fold_strategy=lambda x, y: FoldStrategies.list_of_labels(code_scheme, x, y)
                       ),
                       CodingConfiguration(
                           raw_field=f"facebook_{name}_comment_reply_to_raw",
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.FACEBOOK_COMMENT_REPLY_TO,
                           cleaner=lambda parent: "post" if parent == {} else "comment",
                           coded_field=f"facebook_{name}_comment_reply_to_coded",
                           requires_manual_verification=False,
                           analysis_file_key=f"facebook_{name}_comment_reply_to",
                           fold_strategy=None,
                           include_in_individuals_file=False
                       ),
                       CodingConfiguration(
                           raw_field=f"facebook_{name}_post_raw",
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.FACEBOOK_POST_TYPE,
                           cleaner=facebook_utils.clean_post_type,
                           coded_field=f"facebook_{name}_post_type_coded",
                           requires_manual_verification=False,
                           analysis_file_key=f"facebook_{name}_post_type",
                           fold_strategy=None,
                           include_in_individuals_file=False
                       )
                   ],
                   raw_field_fold_strategy=FoldStrategies.concatenate)


def _make_rqa_coding_plan(name, code_scheme):
    return CodingPlan(dataset_name=f"rqa_{name}",
                      raw_field=f"rqa_{name}_raw",
                      time_field="sent_on",
                      run_id_field=f"rqa_{name}_run_id",
                      coda_filename=f"USAID_IBTCI_rqa_{name}.json",
                      icr_filename=f"rqa_{name}.csv",
                      coding_configurations=[
                          CodingConfiguration(
                              coding_mode=CodingModes.MULTIPLE,
                              code_scheme=code_scheme,
                              coded_field=f"rqa_{name}_coded",
                              analysis_file_key=f"rqa_{name}",
                              fold_strategy=lambda x, y: FoldStrategies.list_of_labels(code_scheme, x, y)
                          )
                      ],
                      ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value(name),
                      raw_field_fold_strategy=FoldStrategies.concatenate)


def get_rqa_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return [
            _make_facebook_coding_plan("s08e01", CodeSchemes.FACEBOOK_S08E01),
            _make_facebook_coding_plan("s08e02", CodeSchemes.FACEBOOK_S08E02),
            _make_facebook_coding_plan("s08e03", CodeSchemes.FACEBOOK_S08E03),

            _make_facebook_coding_plan("s08e03_break_w01", CodeSchemes.FACEBOOK_S08E03_BREAK_W01),
            _make_facebook_coding_plan("s08e03_break_w02", CodeSchemes.FACEBOOK_S08E03_BREAK_W02),
            _make_facebook_coding_plan("s08e03_break_w03", CodeSchemes.FACEBOOK_S08E03_BREAK_W03),
            _make_facebook_coding_plan("s08e03_break_w04", CodeSchemes.FACEBOOK_S08E03_BREAK_W04),
            _make_facebook_coding_plan("s08e03_break_w05", CodeSchemes.FACEBOOK_S08E03_BREAK_W05),
            _make_facebook_coding_plan("s08e03_break_w06", CodeSchemes.FACEBOOK_S08E03_BREAK_W06)
        ]
    else:
        assert pipeline_name == "USAID-IBTCI-SMS"
        return [
            _make_rqa_coding_plan("s08e01", CodeSchemes.RQA_S08E01),
            _make_rqa_coding_plan("s08e02", CodeSchemes.RQA_S08E02),
            _make_rqa_coding_plan("s08e03", CodeSchemes.RQA_S08E03),
            _make_rqa_coding_plan("s08e03_break", CodeSchemes.RQA_S08E03_BREAK),
            _make_rqa_coding_plan("s08e04", CodeSchemes.RQA_S08E04),
            _make_rqa_coding_plan("s08e05", CodeSchemes.RQA_S08E05),
            _make_rqa_coding_plan("s08e06", CodeSchemes.RQA_S08E06),
        ]


def get_demog_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return []
    else:
        assert pipeline_name == "USAID-IBTCI-SMS"
        return [
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
                               fold_strategy=FoldStrategies.assert_label_ids_equal,
                               include_in_theme_distribution=False
                           ),
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.AGE_CATEGORY,
                               coded_field="age_category_coded",
                               analysis_file_key="age_category",
                               fold_strategy=FoldStrategies.assert_label_ids_equal
                           )
                       ],
                       code_imputation_function=code_imputation_functions.impute_age_category,
                       ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
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
                       raw_field_fold_strategy=FoldStrategies.assert_equal),

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
                       raw_field_fold_strategy=FoldStrategies.assert_equal)
        ]


def get_follow_up_coding_plans(pipeline_name):
    return []


def get_engagement_coding_plans(pipeline_name):
    if pipeline_name == "USAID-IBTCI-Facebook":
        return []
    else:
        assert pipeline_name == "USAID-IBTCI-SMS"
        return [
            CodingPlan(dataset_name="rqa_s08e01",
                       raw_field="sent_on",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.ENGAGEMENT_TYPE,
                               cleaner=lambda sent_on: clean_engagement_type(isoparse(sent_on), "rqa_s08e01"),
                               coded_field="rqa_s08e01_engagement_type_coded",
                               analysis_file_key="rqa_s08e01_engagement_type",
                               fold_strategy=None,
                               include_in_individuals_file=False,
                               include_in_theme_distribution=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(dataset_name="rqa_s08e02",
                       raw_field="sent_on",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.ENGAGEMENT_TYPE,
                               cleaner=lambda sent_on: clean_engagement_type(isoparse(sent_on), "rqa_s08e02"),
                               coded_field="rqa_s08e02_engagement_type_coded",
                               analysis_file_key="rqa_s08e02_engagement_type",
                               fold_strategy=None,
                               include_in_individuals_file=False,
                               include_in_theme_distribution=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate),

            CodingPlan(dataset_name="rqa_s08e03",
                       raw_field="sent_on",
                       coding_configurations=[
                           CodingConfiguration(
                               coding_mode=CodingModes.SINGLE,
                               code_scheme=CodeSchemes.ENGAGEMENT_TYPE,
                               cleaner=lambda sent_on: clean_engagement_type(isoparse(sent_on), "rqa_s08e03"),
                               coded_field="rqa_s08e03_engagement_type_coded",
                               analysis_file_key="rqa_s08e03_engagement_type",
                               fold_strategy=None,
                               include_in_individuals_file=False,
                               include_in_theme_distribution=False
                           )
                       ],
                       raw_field_fold_strategy=FoldStrategies.concatenate)
        ]


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET
