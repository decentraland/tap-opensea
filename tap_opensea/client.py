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
        if next_page_token is None:
            last_date = state.get('last_date')
            if last_date:
                params["occurred_after"] = last_date

        if next_page_token:
            params["cursor"] = next_page_token
            self.logger.warning(f"Using cursor {next_page_token}")
        else:
            self.logger.warning(f"No cursor")
        return params

    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=15,
        giveup=lambda e: False,
        factor=6
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


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        state = self.get_context_state(context)
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                continue
            record_timestamp=transformed_record.get('timestamp')
            if record_timestamp:
                recorded_date = state.get('last_date')
                last_date_timestamp = datetime.strptime(record_timestamp[0:10], "%Y-%m-%d").timestamp()
                if last_date_timestamp:
                    last_date = int(last_date_timestamp)
                    if recorded_date:
                        if recorded_date < last_date:
                            state['last_date'] = last_date
                    else:
                        state['last_date'] = last_date
            yield transformed_record