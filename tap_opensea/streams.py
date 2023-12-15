"""Stream type classes for tap-opensea."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
import json
from tap_opensea.client import OpenseaStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class OrdersStream(OpenseaStream):
    name = "opensea_orders_v2"
    path = "/events/collection"
    primary_keys = ["transaction_hash"]
    records_jsonpath = "$.asset_events[*]"
    next_page_token_jsonpath = "$.next"

    @property
    def partitions(self):
        return [{'collection': c} for c in self.config['collections'].split(',')]

    def get_url(self, context: Optional[dict] = None) -> str:
        """Return the API URL."""
        return f"{self.url_base}{self.path}/{context['collection']}"

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert parcels into psv and adds block number"""
        newrow = {}

        # Core
        newrow['transaction_hash'] = str(row['transaction'])
        if 'quantity' in row:
            if row.get('quantity') is not None:
                newrow['quantity'] = int(row.get('quantity'))
        newrow['chain'] = row['chain']

        # NFT
        nft = row.get('nft')

        if nft is not None:
            newrow['nft_address'] = nft.get('contract')
            newrow['nft_collection_name'] = nft.get('collection')
            newrow['nft_identifier'] = nft.get('identifier')
            newrow['nft_name'] = nft.get('name')
            newrow['nft_description'] = nft.get('description')
            newrow['nft_metadata_url'] = nft.get('metadata_url')
            newrow['nft_opensea_url'] = nft.get('opensea_url')

        # Payment
        payment = row.get('payment')
        if payment:
            newrow['payment_symbol'] = payment.get('symbol')
            newrow['payment_token_address'] = payment.get('token_address')
            newrow['payment_amount'] = str(payment.get('quantity'))
            newrow['decimals'] = str(payment.get('decimals'))

        # Seller
        newrow['seller_address'] = row.get('seller')
        newrow['buyer_address'] = row.get('buyer')

        newrow['timestamp'] = str(row.get('event_timestamp'))

        return newrow

    schema = th.PropertiesList(
        th.Property("transaction_hash", th.StringType),
        th.Property("quantity", th.IntegerType),
        th.Property("chain", th.StringType),
        th.Property("nft_address", th.StringType),
        th.Property("nft_collection_name", th.StringType),
        th.Property("nft_identifier", th.StringType),
        th.Property("nft_name", th.StringType),
        th.Property("nft_description", th.StringType),
        th.Property("nft_metadata_url", th.StringType),
        th.Property("nft_opensea_url", th.StringType),
        th.Property("payment_symbol", th.StringType),
        th.Property("payment_token_address", th.StringType),
        th.Property("payment_amount", th.StringType),
        th.Property("decimals", th.StringType),
        th.Property("seller_address", th.StringType),
        th.Property("buyer_address", th.StringType),
        th.Property("timestamp", th.StringType)
    ).to_dict()
