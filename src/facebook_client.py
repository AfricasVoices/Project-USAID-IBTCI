import json

from core_data_modules.data_models import validators
from core_data_modules.logging import Logger
import requests
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

log = Logger(__name__)

_BASE_URL = "https://graph.facebook.com/v8.0"
_MAX_LIMIT = 100

# TODO: Move to a new repo at AfricasVoices/SocialMediaTools
# Included in project source for now because it's likely to evolve very rapidly during the initial experiments.
class FacebookClient(object):
    def __init__(self, access_token):
        self._access_token = access_token

    def _make_get_request(self, endpoint, params):
        params = params.copy()
        params["access_token"] = self._access_token

        next_url = f"{_BASE_URL}{endpoint}"
        result = []
        while next_url is not None:
            print(f"getting {next_url}")
            response = requests.get(next_url, params).json()
            result.extend(response["data"])
            if "paging" in response:
                next_url = response["paging"].get("next")
            else:
                next_url = None
        return result

    def get_all_posts_from_page(self, page_id, fields=["attachments", "created_time", "message"]):
        return self._make_get_request(
            f"/{page_id}/published_posts",
            {
                "fields": ",".join(fields),
                "limit": _MAX_LIMIT
            }
        )

    def get_comments_on_post(self, post_id, fields=["attachments", "created_time", "message"]):
        return self._make_get_request(
            f"/{post_id}/comments",
            {
                "fields": ",".join(fields),
                "limit": _MAX_LIMIT
            }
        )

    def get_all_comments_on_page(self, page_id, fields=["attachments", "created_time", "message"]):
        posts = self.get_all_posts_from_page(page_id)
        comments = []
        for post in [posts[3]]:
            comments.extend(self.get_comments_on_post(post["id"], fields))
        return comments

    @staticmethod
    def convert_facebook_comments_to_traced_data(user, raw_comments):
        log.info(f"Converting {len(raw_comments)} Facebook comments to TracedData...")

        traced_comments = []
        for comment in raw_comments:
            comment["created_time"] = isoparse(comment["created_time"]).isoformat()
            validators.validate_utc_iso_string(comment["created_time"])

            traced_comments.append(TracedData(
                {
                    "avf_facebook_id": None,  # TODO: De-identify a user's FB id here if possible instead
                    "facebook_message_id": comment["id"],
                    "message": comment["message"],
                    "message_created_time": comment["created_time"],
                },
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            ))

        return traced_comments
