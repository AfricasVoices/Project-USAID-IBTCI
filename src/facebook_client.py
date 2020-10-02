import json

from core_data_modules.data_models import validators
from core_data_modules.logging import Logger
import requests
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse

log = Logger(__name__)

_BASE_URL = "https://graph.facebook.com/v8.0"
_MAX_RESULTS_PER_PAGE = 100  # For paged requests, the maximum number of records to request in each page


# TODO: Move to a new repo at AfricasVoices/SocialMediaTools
# Included in project source for now because it's likely to evolve very rapidly during the initial experiments.
class FacebookClient(object):
    def __init__(self, access_token):
        self._access_token = access_token

    def _make_get_request(self, endpoint, params=None):
        if params is None:
            params = {}
        params = params.copy()
        params["access_token"] = self._access_token

        url = f"{_BASE_URL}{endpoint}"
        response = requests.get(url, params)

        return response.json()

    def _make_paged_get_request(self, endpoint, params=None):
        if params is None:
            params = {}
        params = params.copy()
        params["access_token"] = self._access_token

        url = f"{_BASE_URL}{endpoint}"
        response = requests.get(url, params)

        if "data" not in response.json():
            log.error(f"Response from Facebook did not contain a 'data' field. "
                      f"The returned data is probably an error message: {response.json()}")
            exit(1)

        result = response.json()["data"]
        next_url = response.json().get("paging", {}).get("next")
        while next_url is not None:
            response = requests.get(next_url)
            result.extend(response.json()["data"])
            next_url = response.json()["paging"].get("next")
        return result

    def get_post(self, post_id, fields=["created_time", "message", "id"]):
        log.debug(f"Fetching post '{post_id}'...")
        return self._make_get_request(
            f"/{post_id}",
            {
                "fields": ",".join(fields)
            }
        )

    def get_all_posts_published_by_page(self, page_id, fields=["attachments", "created_time", "message"]):
        log.debug(f"Fetching all posts published by page '{page_id}'...")
        posts = self._make_paged_get_request(
            f"/{page_id}/published_posts",
            {
                "fields": ",".join(fields),
                "limit": _MAX_RESULTS_PER_PAGE
            }
        )
        log.info(f"Fetched {len(posts)} posts")
        return posts

    def get_top_level_comments_on_post(self, post_id, fields=["attachments", "created_time", "message"]):
        return self._make_paged_get_request(
            f"/{post_id}/comments",
            {
                "fields": ",".join(fields),
                "limit": _MAX_RESULTS_PER_PAGE,
                "filter": "toplevel"
            }
        )

    def get_all_comments_on_post(self, post_id, fields=["parent", "attachments", "created_time", "message"],
                                 raw_export_log_file=None):
        log.info(f"Fetching all comments on post '{post_id}'...")
        comments = self._make_paged_get_request(
            f"/{post_id}/comments",
            {
                "fields": ",".join(fields),
                "limit": _MAX_RESULTS_PER_PAGE,
                "filter": "stream"
            }
        )
        log.info(f"Fetched {len(comments)} comments")

        if raw_export_log_file is not None:
            log.info(f"Logging {len(comments)} fetched comments...")
            json.dump(comments, raw_export_log_file)
            raw_export_log_file.write("\n")
            log.info(f"Logged fetched comments")
        else:
            log.debug("Not logging the raw export (argument 'raw_export_log_file' was None)")

        return comments

    def get_all_comments_on_page(self, page_id, fields=["parent", "attachments", "created_time", "message"]):
        log.info(f"Fetching all comments on page '{page_id}'...")
        posts = self.get_all_posts_published_by_page(page_id, fields=["id", "is_inline_created"])

        # Posts that were used as adverts are returned twice by Facebook, one post representing the page post and
        # one representing the advert. Both 'posts' have a comments edge pointing to the same dataset, so reading
        # both would lead to duplicated comments in the returned data.
        # We solve this by pre-filtering for posts that were not created inline i.e. we for the original page
        # posts only.
        log.info("Filtering out posts that were created inline")
        posts = [p for p in posts if p.get("is_inline_created") == False]
        log.info(f"Filtered out posts that were created inline. {len(posts)} remain")

        comments = []
        for post in posts:
            comments.extend(self.get_all_comments_on_post(post["id"], fields))
        log.info(f"Fetched {len(comments)} on page '{page_id}' (from {len(posts)} posts)")
        return comments

    @staticmethod
    def convert_facebook_comments_to_traced_data(user, dataset_name, raw_comments):
        log.info(f"Converting {len(raw_comments)} Facebook comments to TracedData...")

        traced_comments = []
        # Use a placeholder avf facebook id for now, to make the individuals file work until we know if we'll be able
        # to see Facebook user ids or not.
        # TODO: If we get ids from FB, de-identify instead of incrementing this placeholder;
        #       If we don't get ids from FB, re-configure the pipeline to make processing individuals optional
        avf_facebook_id = 0
        for comment in raw_comments:
            comment["created_time"] = isoparse(comment["created_time"]).isoformat()
            validators.validate_utc_iso_string(comment["created_time"])

            comment_dict = {
                "avf_facebook_id": f"{dataset_name}_{avf_facebook_id}",  # TODO: De-identify a user's FB id here if possible instead
            }
            for k, v in comment.items():
                comment_dict[f"{dataset_name}.{k}"] = v

            traced_comments.append(
                TracedData(comment_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))
            avf_facebook_id += 1

        log.info(f"Converted {len(traced_comments)} Facebook comments to TracedData")

        return traced_comments
