import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    RQA_S08E01 = _open_scheme("rqa_s08e01.json")
    RQA_S08E02 = _open_scheme("rqa_s08e02.json")
    RQA_S08E03 = _open_scheme("rqa_s08e03.json")
    RQA_S08E03_BREAK = _open_scheme("rqa_s08e03_break.json")
    RQA_S08E04 = _open_scheme("rqa_s08e04.json")
    RQA_S08E05 = _open_scheme("rqa_s08e05.json")
    RQA_S08E06 = _open_scheme("rqa_s08e06.json")

    SOMALIA_OPERATOR = _open_scheme("somalia_operator.json")

    AGE = _open_scheme("age.json")
    AGE_CATEGORY = _open_scheme("age_category.json")
    GENDER = _open_scheme("gender.json")
    MOGADISHU_SUB_DISTRICT = _open_scheme("mogadishu_sub_district.json")
    SOMALIA_DISTRICT = _open_scheme("somalia_district.json")
    SOMALIA_REGION = _open_scheme("somalia_region.json")
    SOMALIA_STATE = _open_scheme("somalia_state.json")
    SOMALIA_ZONE = _open_scheme("somalia_zone.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
    LIVELIHOOD = _open_scheme("livelihood.json")
    HOUSEHOLD_LANGUAGE = _open_scheme("household_language.json")

    S08_HAVE_VOICE = _open_scheme("s08_have_voice.json")
    S08_SUGGESTIONS = _open_scheme("s08_suggestions.json")

    ENGAGEMENT_TYPE = _open_scheme("engagement_type.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")

    FACEBOOK_S08E01 = _open_scheme("facebook_s08e01.json")
    FACEBOOK_S08E02 = _open_scheme("facebook_s08e02.json")
    FACEBOOK_S08E03 = _open_scheme("facebook_s08e03.json")
    FACEBOOK_S08E03_BREAK_W01 = _open_scheme("facebook_s08e03_break_w01.json")
    FACEBOOK_S08E03_BREAK_W02 = _open_scheme("facebook_s08e03_break_w02.json")
    FACEBOOK_S08E03_BREAK_W03 = _open_scheme("facebook_s08e03_break_w03.json")
    FACEBOOK_S08E03_BREAK_W04 = _open_scheme("facebook_s08e03_break_w04.json")
    FACEBOOK_S08E03_BREAK_W05 = _open_scheme("facebook_s08e03_break_w05.json")
    FACEBOOK_S08E03_BREAK_W06 = _open_scheme("facebook_s08e03_break_w06.json")
    FACEBOOK_S08E04 = _open_scheme("facebook_s08e04.json")
    FACEBOOK_S08E05 = _open_scheme("facebook_s08e05.json")
    FACEBOOK_S08E06 = _open_scheme("facebook_s08e06.json")
    FACEBOOK_S08E06_BREAK_W01 = _open_scheme("facebook_s08e06_break_w01.json")
    FACEBOOK_S08E06_BREAK_W02 = _open_scheme("facebook_s08e06_break_w03.json")
    FACEBOOK_S08E06_BREAK_W03 = _open_scheme("facebook_s08e06_break_w03.json")


    FACEBOOK_COMMENT_REPLY_TO = _open_scheme("facebook_comment_reply_to.json")
    FACEBOOK_POST_TYPE = _open_scheme("facebook_post_type.json")
