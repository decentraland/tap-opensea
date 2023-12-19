"""opensea tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_opensea.streams import (
    OrdersStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.


STREAM_TYPES = [
    OrdersStream
]


class Tapopensea(Tap):
    """opensea tap class."""
    name = "tap-opensea"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType),
        th.Property("api_url", th.StringType, default="https://api.opensea.io/api/v2"),
        th.Property("collections", th.StringType, default="dcl-names"),
        th.Property("start_date", th.StringType, default="2023-12-10"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
