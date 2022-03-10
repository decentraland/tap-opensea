"""Stream type classes for tap-opensea."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
import json
from tap_opensea.client import openseaStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class OrdersStream(openseaStream):
    """Orders from OpenSea"""
    name = "opensea_orders"
    path = "/events"
    primary_keys = ["id"]
    records_jsonpath = "$.asset_events[*]"
    next_page_token_jsonpath = "$.next"

    @property
    def partitions(self):
        return [{'collection': c} for c in self.config['collections'].split(',')]

    def transform_asset(self, asset):
        outAsset = {}
        collection = asset.get('collection')
        outAsset['id'] = str(asset.get('id'))
        outAsset['collection'] = collection.get('slug')
        outAsset['name'] = asset.get('name')
        outAsset['external_link'] = asset.get('external_link')
        outAsset['permalink'] = asset.get('permalink')

        return outAsset

    
    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert parcels into psv and adds block number"""
        newrow = {}
        
        # Core
        newrow['id'] = str(row['id'])
        newrow['auction_type'] = row['auction_type']
        newrow['quantity'] = int(row['quantity'])

        # Payment
        payment = row.get('payment_token')
        newrow['payment_symbol'] = payment.get('symbol')
        newrow['payment_amount_eth'] = payment.get('eth_price')
        newrow['payment_amount_usd'] = payment.get('usd_price')

        # Seller
        seller = row.get('seller')
        newrow['seller_address'] = seller.get('address')
        seller_user = seller.get('user')
        if seller_user:
            newrow['seller_username'] = seller_user.get('username')

        # Buyer
        buyer = row.get('winner_account')
        newrow['buyer_address'] = buyer.get('address')
        buyer_user = buyer.get('user')
        if buyer_user:
            newrow['buyer_username'] = buyer_user.get('username')

        # Transaction
        transaction = row.get('transaction')
        newrow['timestamp'] = transaction.get('timestamp')
        newrow['transaction_hash'] = transaction.get('transaction_hash')

        # Asset
        newrow['assets'] = []
        asset_bundle = row.get('asset_bundle')
        if asset_bundle is not None:
            assets = asset_bundle.get('assets')
            for asset in assets:
                newrow['assets'].append(self.transform_asset(asset))
        else:
            asset = row.get('asset')
            newrow['assets'].append(self.transform_asset(asset))

        return newrow

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("transaction_hash", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("auction_type", th.StringType),
        th.Property("payment_symbol", th.StringType),
        th.Property("payment_amount_eth", th.StringType),
        th.Property("payment_amount_usd", th.StringType),
        th.Property("quantity", th.IntegerType),
        th.Property("seller_address", th.StringType),
        th.Property("seller_username", th.StringType),
        th.Property("buyer_address", th.StringType),
        th.Property("buyer_username", th.StringType),
        th.Property("assets", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("token_id", th.StringType),
            th.Property("collection", th.StringType),
            th.Property("name", th.StringType),
        ))),
    ).to_dict()

