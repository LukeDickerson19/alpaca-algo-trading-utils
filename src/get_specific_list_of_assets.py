
'''

    Description
	
        get list of assets instead of just one
        FAILED TO DO THIS :(
        i had to just get all the assets though and filter for the list i need in one api call
	
	sources:
	
        get list of assets
		https://docs.alpaca.markets/reference/get-v2-assets-1        
        
        get just one asset
		https://docs.alpaca.markets/reference/get-v2-assets-symbol_or_asset_id
		https://alpaca.markets/sdks/python/api_reference/trading/assets.html#get-asset

    '''

# standard libraries
import os
import json
import sys
import time
import requests
import pathlib
REPO_PATH = str(pathlib.Path(__file__).resolve().parent.parent)
DATA_PATH = os.path.join(REPO_PATH, "data", "ticker_data")
# print('REPO_PATH', REPO_PATH)
# print('DATA_PATH', DATA_PATH)
# sys.exit()

# non-standard libraries
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass


# API constants
LIVE_TRADING = False
with open('credentials.json') as f:
	creds = json.load(f)
ENDPOINT   = creds['live_trading' if LIVE_TRADING else 'paper_trading']['ENDPOINT']
API_KEY    = creds['live_trading' if LIVE_TRADING else 'paper_trading']['API_KEY_ID']
API_SECRET = creds['live_trading' if LIVE_TRADING else 'paper_trading']['SECRET_KEY']


trading_client = TradingClient(API_KEY, API_SECRET)

# search for stock assets
# https://alpaca.markets/sdks/python/api_reference/trading/assets.html
assets_to_get = ['AAPL', 'JNJ', 'CVX']
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
assets = {asset.symbol : asset for asset in \
    trading_client.get_all_assets(search_params)}
for symbol in assets_to_get:
	asset = assets[symbol]
	print(asset)


# # THIS FAILED
# # url = "https://paper-api.alpaca.markets/v2/assets?attributes="
# symbols = '%2C'.join(ticker_symbols) # ex: AAPL%2CTSLA%2CMSFT
# # url = f"https://data.alpaca.markets/v2/stocks/quotes/latest?symbols={symbols}&feed={exchange}"
# url = f"https://paper-api.alpaca.markets/v2/assets/symbols={symbols}"#&feed={exchange}"
# response = requests.get(url, headers=HEADERS)
# print(response.text)
# assets = json.loads(response.text)
# for asset_dct in assets:
# 	print(json.dumps(asset_dct, indent=4))
	
