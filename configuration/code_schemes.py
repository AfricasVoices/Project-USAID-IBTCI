import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    FACEBOOK_S01E01 = _open_scheme("facebook_s01e01.json")

    WS_CORRECT_DATASET = None  # TODO Add scheme
