
'''

	Description:
		pipeline to get price history of US equities on Alpaca
        on specified
            start_date
            end_date
            interval

	Sources:
        https://docs.alpaca.markets/reference/stockbars
		https://docs.alpaca.markets/docs/market-data-faq

	'''

# standard libraries
import os
import json
import sys
import time
import requests
import pathlib
REPO_PATH = str(pathlib.Path(__file__).resolve().parent.parent)
DATA_PATH = os.path.join(REPO_PATH, "data", "price_data")
# print('REPO_PATH', REPO_PATH)
# print('DATA_PATH', DATA_PATH)
# sys.exit()

# non-standard libraries
from datetime import datetime, timedelta
import pandas as pd
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 1000)
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
HEADERS = {
    "accept": "application/json",
    "APCA-API-KEY-ID": API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET
}

trading_client = TradingClient(API_KEY, API_SECRET)


input_filename = "all_shortable_alpaca_stocks.csv"
# filename = "all_alpaca_stocks.csv"
# filename = "all_fractional_and_non_fractionable_alpaca_stocks.csv"
df0 = pd.read_csv(os.path.join(REPO_PATH, "data", "ticker_data", input_filename))
df0.sort_values(by=['ticker'], inplace=True)
# print(df0)

now = datetime.now()
end_date = now.strftime('%Y-%m-%d')
start_date = (now - timedelta(days=365)).strftime('%Y-%m-%d') # one year ago
# end_date = ... if end_date is excluded, the current time is assumed
interval = '1Day' # see here for valid intervals: https://docs.alpaca.markets/reference/stockbars
exchange = 'iex' # 'sip'
def get_price_history(ticker):
    url = f"https://data.alpaca.markets/v2/stocks/bars?symbols={ticker}&timeframe={interval}&start={start_date}&end={end_date}&limit=1000&adjustment=all&feed={exchange}&sort=asc"
    response = requests.get(url, headers=HEADERS)
    # NOTE: if you request more than 1000 data points (in total, not per symbol), you'll have to concatinate paginated responses
    data = json.loads(response.text)
    # print(json.dumps(data, indent=4))
    if "bars" not in data.keys() or ticker not in data["bars"].keys():
        print(f"no price data found for {ticker}")
        return None
    df = pd.DataFrame(data['bars'][ticker])
    # i asked in alpaca's slack what "n" stands for and their AI said
    # "number of trades that occurred during the bar's time period"
    # and it said "vw" was "volume-weighted average price of the stock during the bar's time period."
    # https://alpaca-community.slack.com/archives/CEL9HCSN4/p1704602713177009
    df.rename(columns={
         'c' : "close",
         'h' : "high",
         'l' : "low",
         'n' : "number_of_trades",
         'o' : "open",
         't' : "time",
         'v' : "volume",
         'vw' : "volumn_weighted_average_price"
    }, inplace=True)
    reordered_columns = [
         "time",
         "open",
         "high",
         "low",
         "close",
         "number_of_trades",
         "volume",
         "volumn_weighted_average_price"
    ]
    df = df[reordered_columns]
    return df

''' get_ohlcv_candle_history()
        description:
            get candle data for specified ticker symbol(s), start datetime, end datetime, and candle interval
        args:
            symbol - string - ticker symbol to get candles of
            start_date - datetime - start date of candle data
            end_date - datetime - end date of candle data
            candle_interval - string - interval for each price candle
                valid values:
                    '1 minute'
                    '5 minutes'
                    '10 minutes'
                    '15 minutes'
                    '30 minutes'
                    '1 day'
                    '1 week'
                for Alpaca, more valid candle intervals can be found here:
                https://docs.alpaca.markets/reference/stockbars
                    excerpt:
                        The timeframe represented by each bar in aggregation.
                            You can use any of the following values:

                            [1-59]Min or [1-59]T, e.g. 5Min or 5T creates 5-minute aggregations
                            [1-23]Hour or [1-23]H, e.g. 12Hour or 12H creates 12-hour aggregations
                            1Day or 1D creates 1-day aggregations
                            1Week or 1W creates 1-week aggregations
                            [1,2,3,4,6,12]Month or [1,2,3,4,6,12]M, e.g. 3Month or 3M creates 3-month aggregations
            extended_hours - boolean - flag to get extended hours data
        returns:
            dictionary with:
                keys - string - each symbol requested
                values - pandas dataframe - with the candle data with the requested start time, end time, and interval
                    dataframe columns:
                        time
                        open
                        high
                        low
                        close
                        number_of_trades
                        volume
    '''
def get_ohlcv_candle_history(self, symbol, start_date, end_date, candle_interval, extended_hours=True):
    # see here for all valid intervals: https://docs.alpaca.markets/reference/stockbars
    if candle_interval == '1 minute':
        api_interval_str = '1Min'
    elif candle_interval == '5 minutes':
        api_interval_str = '5Min'
    elif candle_interval == '10 minutes':
        api_interval_str = '10Min'
    elif candle_interval == '15 minutes':
        api_interval_str = '15Min'
    elif candle_interval == '30 minutes':
        api_interval_str = '30Min'
    elif candle_interval == '1 day':
        api_interval_str = '1Day'
    elif candle_interval == '1 week':
        api_interval_str = '1Week'
    else:
        print('invalid candle interval')
        sys.exit()
    exchange = 'sip' # sip is free for the datafeed, but not for trading
    df = None
    next_page_token = ''
    while next_page_token != None:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d') # NOTE: end date is inclusive
        url = f"https://data.alpaca.markets/v2/stocks/bars?symbols={symbol}&timeframe={api_interval_str}&start={start_date_str}&end={end_date_str}&limit=1000&adjustment=split&feed={exchange}&page_token={next_page_token}&sort=asc"
        response = requests.get(url, headers=self.headers)
        # NOTE: if you request more than 1000 ohlcv rows (rows aka candles) (1000 in total, not per symbol), you'll have to concatinate paginated responses
        time.sleep(1)
        data = json.loads(response.text)
        # print(json.dumps(data, indent=4))
        if "bars" not in data.keys() or symbol not in data["bars"].keys():
            print(f"            no price data found for {symbol} between {start_date_str} and {end_date_str}")
            return None
        df = pd.concat([df, pd.DataFrame(data['bars'][symbol])])
        next_page_token = data['next_page_token']
        # print(pd.DataFrame(data['bars'][symbol]))
        # print('next_page_token', next_page_token)
        # input()
    # i asked in alpaca's slack what "n" stands for and their AI said
    # "number of trades that occurred during the bar's time period"
    # and it said "vw" was "volume-weighted average price of the stock during the bar's time period."
    # https://alpaca-community.slack.com/archives/CEL9HCSN4/p1704602713177009
    df.rename(columns={
        'c' : "close",
        'h' : "high",
        'l' : "low",
        # 'n' : "number_of_trades", # NOTE: excluded to match return columns of SchwabExchange.get_ohlcv_candle_history
        'o' : "open",
        't' : "time",
        'v' : "volume",
        # 'vw' : "volumn_weighted_average_price" # NOTE: excluded to match return columns of SchwabExchange.get_ohlcv_candle_history
    }, inplace=True)
    reordered_columns = [
        "time",
        "open",
        "high",
        "low",
        "close",
        # "number_of_trades",
        "volume",
        # "volumn_weighted_average_price"
    ]
    df = df[reordered_columns]
    stock_market_timezone = 'America/New_York'
    df['time'] = pd.to_datetime(
        df['time'],
        format='%Y-%m-%dT%H:%M:%SZ',
        errors='coerce',
        utc=True).dt.tz_convert(stock_market_timezone) # convert string to datetime
    # print(df)
    return df



if __name__ == '__main__':

    output_dir_name = f"price_data_for_1_year_on_daily_intervals_from_{start_date}_to_{end_date}_of_shortable_alpaca_stocks"
    output_dir_path = os.path.join(DATA_PATH, output_dir_name)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    for i, row in df0.iterrows():
        # if i < 1975: continue # for testing purposes only, also this script sometimes gets stuck
        ticker = row['ticker']
        print(f"ticker {i + 1} of {df0.shape[0]}: {ticker}")
        df = get_price_history(ticker)
        if isinstance(df, pd.DataFrame):
            df.to_csv(os.path.join(output_dir_path, f"{ticker}.csv"), index=False)
        time.sleep(1)

