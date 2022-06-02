"""REST client handling, including openseaStream base class."""

import requests, pendulum
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from tap_opensea.auth import openseaAuthenticator
from datetime import datetime, timedelta, date, time, timezone
import backoff

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class openseaStream(RESTStream):
    """opensea stream class."""

    url_base = "https://api.opensea.io/api/v1"
    
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
        signpost = datetime.combine(date.today(), time()).replace(tzinfo=timezone.utc).timestamp()
        
        params: dict = {
           "only_opensea": "true",
           "collection_slug": context['collection'],
           "event_type": "successful",
           "occurred_before": str(signpost)
        }

        # Cursor
        if next_page_token is not None:
            state['cursor_key'] = next_page_token
            cursor = next_page_token
        else:
            cursor = state.get('cursor_key')

        if cursor:
            params["cursor"] = cursor
            self.logger.warning(f"Using cursor {cursor}")
        else:
            self.logger.warning(f"No cursor")
        return params

    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=10,
        giveup=lambda e: False,
        factor=3,
    )
    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        self.logger.debug("Response received successfully.")
        return response
