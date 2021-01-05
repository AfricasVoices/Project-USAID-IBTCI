

class FacebookUtils(object):
    @staticmethod
    def clean_post_type(post):
        """
        Cleans Facebook post type

        :param post: Facebook post in the format returned by Facebook's API.
        :type post: dict
        :return: "photo" | "video" | None
        :rtype: str | None
        """
        post_type = None
        for attachment in post["attachments"]["data"]:
            assert attachment["type"] in {"video_inline", "video_direct_response", "photo"}, post

            if attachment["type"] in {"video_inline", "video_direct_response"}:
                assert post_type in {"video", None}, post
                post_type = "video"
            elif attachment["type"] == "photo":
                assert post_type in {"photo", None}, post
                post_type = "photo"

        return post_type
