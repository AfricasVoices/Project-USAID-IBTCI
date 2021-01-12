import json

import pytz
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

    @staticmethod
    def _date_to_facebook_time(date):
        """
        Converts a datetime into a format compatible with Facebook's API.

        Facebook only accepts dates as ISO 8601 strings in Zulu-time (UTC with a 'Z' indicator of timezone).

        :param date: Date to convert to Facebook time.
        :type date: datetime
        :return: `dat` in a format compatible with Facebook's APIs.
        :rtype: str
        """
        return date.astimezone(pytz.utc).isoformat().replace("+00:00", "Z")

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

    def get_all_posts_published_by_page(self, page_id, fields=["attachments", "created_time", "message"],
                                        created_after=None, created_before=None):
        log_str = f"Fetching all posts published by page '{page_id}'"
        if created_after is not None:
            log_str += f", created after {created_after.isoformat()}"
        if created_before is not None:
            log_str += f", created after {created_before.isoformat()}"
        log.debug(f"{log_str}...")

        params = {
            "fields": ",".join(fields),
            "limit": _MAX_RESULTS_PER_PAGE
        }
        if created_after is not None:
            params["since"] = self._date_to_facebook_time(created_after)
        if created_before is not None:
            params["until"] = self._date_to_facebook_time(created_before)

        posts = self._make_paged_get_request(
            f"/{page_id}/published_posts",
            params
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

    def get_raw_metrics_for_post(self, post_id, metrics):
        return self._make_get_request(
            f"/{post_id}/insights?metric={','.join(metrics)}"
        )["data"]

    def get_metrics_for_post(self, post_id, metrics):
        raw_metrics = self.get_raw_metrics_for_post(post_id, metrics)

        cleaned_metrics = dict()
        for m in raw_metrics:
            assert len(m["values"]) == 1, f"Metric {m['name']} has {len(m['values'])} values, but " \
                                          f"FacebookClient.get_metrics_for_post only expects one. " \
                                          f"Use `FacebookClient.get_raw_metrics_for_post` instead or report" \
                                          f"this to the developers of FacebookClient"
            cleaned_metrics[m["name"]] = m["values"][0]["value"]

        return cleaned_metrics

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
    def convert_facebook_comments_to_traced_data(user, dataset_name, raw_comments, facebook_uuid_table):
        log.info(f"Converting {len(raw_comments)} Facebook comments to TracedData...")

        facebook_uuids = {comment["from"]["id"] for comment in raw_comments}
        facebook_to_uuid_lut = facebook_uuid_table.data_to_uuid_batch(facebook_uuids)

        traced_comments = []
        # Use a placeholder avf facebook id for now, to make the individuals file work until we know if we'll be able
        # to see Facebook user ids or not.
        for comment in raw_comments:
            comment["created_time"] = isoparse(comment["created_time"]).isoformat()
            validators.validate_utc_iso_string(comment["created_time"])

            comment_dict = {
                "avf_facebook_id": facebook_to_uuid_lut[comment["from"]["id"]]
            }
            for k, v in comment.items():
                comment_dict[f"{dataset_name}.{k}"] = v

            traced_comments.append(
                TracedData(comment_dict,
                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

        log.info(f"Converted {len(traced_comments)} Facebook comments to TracedData")

        return traced_comments
