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

    SOMALIA_OPERATOR = _open_scheme("somalia_operator.json")

    AGE = _open_scheme("age.json")
    GENDER = _open_scheme("gender.json")
    MOGADISHU_SUB_DISTRICT = _open_scheme("mogadishu_sub_district.json")
    SOMALIA_DISTRICT = _open_scheme("somalia_district.json")
    SOMALIA_REGION = _open_scheme("somalia_region.json")
    SOMALIA_STATE = _open_scheme("somalia_state.json")
    SOMALIA_ZONE = _open_scheme("somalia_zone.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
    LIVELIHOOD = _open_scheme("livelihood.json")
    HOUSEHOLD_LANGUAGE = _open_scheme("household_language.json")

    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")

    FACEBOOK_S01E01 = _open_scheme("facebook_s01e01.json")
    FACEBOOK_S01E02 = _open_scheme("facebook_s01e02.json")
    FACEBOOK_COMMENT_REPLY_TO = _open_scheme("facebook_comment_reply_to.json")
