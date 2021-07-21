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
    limit_rows = 300
    
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

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        length_results = sum(1 for r in extract_jsonpath(self.records_jsonpath, input=response.json()))
        last_offset = previous_token or 0

        if length_results < self.limit_rows or length_results == 0:
            return None

        return last_offset + self.limit_rows



    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        
        state = self.get_context_state(context)
        signpost = datetime.combine(date.today(), time()).replace(tzinfo=timezone.utc)
        start_key_str = state.get('last_end_key')
        if start_key_str:
            start_key = cast(datetime, pendulum.parse(start_key_str))
        else:
            start_key = datetime(year=2018, month=1, day=1, tzinfo=timezone.utc)

        end_key = start_key + timedelta(days=30)
        if end_key > signpost:
            end_key = signpost
        
        state['last_end_key'] = end_key.strftime("%Y-%m-%dT%H:%M:%S")

        start_ts = start_key.timestamp()
        end_ts = end_key.timestamp()
        offset = next_page_token or 0

        params: dict = {
           "only_opensea": "true",
           "offset": str(offset),
           "limit": str(self.limit_rows),
           "collection_slug": context['collection'],
           "event_type": "successful",
           "occurred_after": str(start_ts),
           "occurred_before": str(end_ts)
        }
        return params

    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500 and e.respone.status_code != 429,
        factor=2,
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
