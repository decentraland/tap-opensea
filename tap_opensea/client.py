"""REST client handling, including openseaStream base class."""


import time
from pathlib import Path
from typing import Any, Dict, Optional, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from tap_opensea.auth import openseaAuthenticator
from datetime import datetime, timedelta, date, time, timezone


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class OpenseaStream(RESTStream):
    """opensea stream class."""

    url_base = "https://api.opensea.io/api/v2"

    @property
    def authenticator(self) -> openseaAuthenticator:
        """Return a new authenticator object."""
        return openseaAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""

        state = self.get_context_state(context)
        signpost = datetime.combine(date.today(), time()).replace(
            tzinfo=timezone.utc).timestamp()

        params: dict = {
            "event_type": "sale",
            "before": str(signpost)
        }

        # Cursor
        if next_page_token is None:
            last_date = state.get('last_date')
            if last_date:
                params["after"] = last_date

        if next_page_token:
            params["next"] = next_page_token
            self.logger.warning(f"Using cursor {next_page_token}")
        else:
            self.logger.warning(f"No cursor")
        return params

    def backoff_max_tries(self):
        return 20

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        state = self.get_context_state(context)
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                continue

            # Update state with last date
            if 'timestamp' in transformed_record:
                state['last_date'] = transformed_record['timestamp']

            yield transformed_record
