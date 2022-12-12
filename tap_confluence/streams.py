from __future__ import annotations

import abc
from base64 import b64encode
from pathlib import Path
from typing import Any, Dict, Iterable, List

from singer_sdk import typing as th  # JSON Schema typing helpers

import requests
from singer_sdk.streams import RESTStream


class TapConfluenceStream(RESTStream):

    limit: int = 100
    expand: List[str] = []

    @property
    def url_base(self) -> str:
        """Return the base Confluence URL."""
        return self.config.get("base_url")

    @property
    def http_headers(self) -> dict:
        result = super().http_headers

        email = self.config.get("email")
        api_token = self.config.get("api_token")
        auth = b64encode(f"{email}:{api_token}".encode()).decode()

        result["Authorization"] = f"Basic {auth}"

        return result

    def get_url_params(
        self,
        partition: dict | None,
        next_page_token: int | None,
    ) -> Dict[str, Any]:
        return {
            "limit": self.limit,
            "start": next_page_token,
            "expand": ",".join(self.expand),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["results"]:
            # self.logger.info(row.keys())
            # self.logger.info(row.get("_expandable"))
            # self.logger.info(row["_links"])
            yield row

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: int | None,
    ) -> int | None:

        previous_token = previous_token or 1

        data = response.json()
        size, limit = data["size"], data["limit"]

        if size < limit:
            return None

        return previous_token + limit


class GroupsStream(TapConfluenceStream):
    name = "groups"
    path = "/group"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("_links", th.ObjectType(
            th.Property("self", th.StringType)
        ))
    ).to_dict()


class SpacesStream(TapConfluenceStream):
    name = "spaces"
    path = "/space"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("permissions", th.ArrayType(
            th.ObjectType(
                th.Property("subjects", th.ObjectType(
                    th.Property("user", th.ObjectType(
                        th.Property("results", th.ArrayType(th.ObjectType(
                            th.Property("accountId", th.StringType),
                            th.Property("email", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("publicName", th.StringType),
                        )))
                    )),
                    th.Property("group", th.ObjectType(
                        th.Property("results", th.ArrayType(th.ObjectType(
                            th.Property("id", th.StringType),
                            th.Property("name", th.StringType),
                            th.Property("type", th.StringType),
                        ))
                    )),
                )),
                th.Property("anonymousAccess", th.BooleanType),
                th.Property("unlicensedAccess", th.BooleanType),
                th.Property("operation", th.ObjectType(
                    th.Property("operation", th.StringType),
                    th.Property("targetType", th.StringType),
                )),
            )
        )),
        th.Property("icon", th.ObjectType(
            th.Property("path", th.StringType),
            th.Property("width", th.IntegerType),
            th.Property("height", th.IntegerType),
            th.Property("isDefault", th.BooleanType),
        )),
        th.Property("description", th.ObjectType(
            th.Property("plain", th.ObjectType(
                th.Property("representation", th.StringType),
                th.Property("value", th.StringType),
            )),
            th.Property("view", th.ObjectType(
                th.Property("representation", th.StringType),
                th.Property("value", th.StringType),
            )),
        )),
        th.Property("_expandable", th.ObjectType(
            th.Property("homepage", th.StringType),
        )),
        th.Property("_links", th.ObjectType(
            th.Property("self", th.StringType),
            th.Property("webui", th.StringType),
        )),
    ).to_dict()


class ThemesStream(TapConfluenceStream):
    name = "themes"
    path = "/settings/theme"
    primary_keys = ["themeKey"]
    schema = th.PropertiesList(
        th.Property("themeKey", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("icon", th.ObjectType(
            th.PropertyType("path", th.StringType),
            th.PropertyType("width", th.IntegerType),
            th.PropertyType("height", th.IntegerType),
            th.PropertyType("isDefault", th.BooleanType),
        )),
    ).to_dict()

        
class BaseContentStream(TapConfluenceStream, metaclass=abc.ABCMeta):
    path = "/content"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("title", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("history", th.ObjectType(
            th.Property("latest", th.BooleanType),
            th.Property("createdBy", th.ObjectType(
                th.PropertyType("type", th.StringType),
                th.PropertyType("accountId", th.StringType),
                th.PropertyType("email", th.StringType),
                th.PropertyType("publicName", th.StringType),
            )),
            th.Property("createdDate", th.DateTimeType),
            th.Property("contributors", th.ObjectType(
                th.Property("publishers", th.ObjectType(
                    th.Property("users", th.ArrayType(
                        th.ObjectType(
                            th.PropertyType("type", th.StringType),
                            th.PropertyType("accountId", th.StringType),
                            th.PropertyType("email", th.StringType),
                            th.PropertyType("publicName", th.StringType),
                        )
                    )),
                    th.Property("userKeys", th.ArrayType(th.StringType)),
                )),
            )),
            th.Property("previousVersion", th.ObjectType(
    th.Property("by", th.ObjectType(
        th.PropertyType("type", th.StringType),
        th.PropertyType("accountId", th.StringType),
        th.PropertyType("email", th.StringType),
        th.PropertyType("publicName", th.StringType),
     )
   ),
    th.Property("when", th.DateTimeType),
    th.Property("friendlyWhen", th.StringType),
    th.Property("message", th.StringType),
    th.Property("number", th.IntegerType),
    th.Property("minorEdit", th.BooleanType),
    th.Property("collaborators", th.ObjectType(
                    th.Property("users", th.ArrayType(
                        th.ObjectType(
                            th.PropertyType("type", th.StringType),
                            th.PropertyType("accountId", th.StringType),
                            th.PropertyType("email", th.StringType),
                            th.PropertyType("publicName", th.StringType),
                        )
                    )),
                    th.Property("userKeys", th.ArrayType(th.StringType)),
                )),
)
),
        )),
        th.Property("version", th.ObjectType(
    th.Property("by", th.ObjectType(
        th.PropertyType("type", th.StringType),
        th.PropertyType("accountId", th.StringType),
        th.PropertyType("email", th.StringType),
        th.PropertyType("publicName", th.StringType),
     )
   ),
    th.Property("when", th.DateTimeType),
    th.Property("friendlyWhen", th.StringType),
    th.Property("message", th.StringType),
    th.Property("number", th.IntegerType),
    th.Property("minorEdit", th.BooleanType),
    th.Property("collaborators", th.ObjectType(
                    th.Property("users", th.ArrayType(
                        th.ObjectType(
                            th.PropertyType("type", th.StringType),
                            th.PropertyType("accountId", th.StringType),
                            th.PropertyType("email", th.StringType),
                            th.PropertyType("publicName", th.StringType),
                        )
                    )),
                    th.Property("userKeys", th.ArrayType(th.StringType)),
                )),
)
),
    th.Property("descendants", th.ObjectType(
        th.Property("results", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("title", th.StringType),
                th.Property("type", th.StringType),
                th.Property("status", th.StringType),
            )
        ))
    )),
        th.Property("restrictions", th.ObjectType(
            th.Property("operations", th.StringType),
#             th.Property("restrictions", -- To Check
        )),
        th.Property("_expandable", th.ObjectType(
            th.Property("container", th.StringType),
            th.Property("space", th.StringType),
        )),
        th.Property("_links", th.ObjectType(
            th.Property("self", th.StringType),
            th.Property("tinyui", th.StringType),
            th.Property("editui", th.StringType),
            th.Property("webui", th.StringType),
        )),
    ).to_dict()

    @property
    @abc.abstractmethod
    def content_type(self) -> str:
        """Content type (page or blogpost)."""
        pass

    def get_url_params(
        self,
        partition: dict | None,
        next_page_token: int | None,
    ) -> Dict[str, Any]:
        result = super().get_url_params(partition, next_page_token)
        result["type"] = self.content_type
        return result

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["type"] = self.content_type
        return row


class PagesStream(BaseContentStream):
    name = "pages"
    content_type = "page"


class BlogpostsStream(BaseContentStream):
    name = "blogposts"
    content_type = "blogpost"
