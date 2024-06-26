"""

    Description:

        This code uses 2 threads to subscribe to a websocket stream of new quotes, and another websocket
        stream of trade updates for stocks in SYMBOLS list, both from the Alpaca API. Threads should be
        used instead of multiprocessing processes when you need both threads to be able modify the global
        variables in a shared state.

        Collecting quote (aka price) and trade updates by listening to websocket streams instead of
        querying the rest API every 10 seconds is done to minimize the likelyhood of hitting the Alpaca
        API rate limit. The free tier allowes 200 requests per minute, and the $100/month paid tier allows
        1000 requests per minute. Also the trading bot can react faster to immediate quotes. However the
        websocket data must be collected in separate threads because the script will be blocked after calling
        the "*_websocket_client.run()" function. Errors in threads will print to console and will stop the
        thread, but not the main parent thread. Errors in main thread are handled with a try/except block.
        source: convo with kapa.ai: https://alpaca-community.slack.com/archives/CEL9HCSN4/p1708615661484309

        
    Sources:

        https://alpaca.markets/sdks/python/api_reference/data/stock/live.html


"""
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import threading
import asyncio
import traceback
from alpaca.data.live.stock import StockDataStream
from alpaca.data.enums import DataFeed
from alpaca.trading.stream import TradingStream


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

TIMEZONE = 'US/Eastern' # 'US/Pacific' # 'UTC'
DATE_FMT = '%Y-%m-%d %H:%M:%S %Z'
SYMBOLS = [
    "AAPL",
    "LMT",
    "CVX",
    "COST",
    "TXN",
]

# thread test functions
async def quote_stream_test(quote):
    quote_received_time = datetime.now(ZoneInfo(TIMEZONE)).strftime(DATE_FMT)
    print('test quote update', quote_received_time, quote)
    # try:
    #     print('test quote update', quote_received_time, quote)
    # except:
    #     print(f'\nException in quote_stream!!!')
    #     print(f'{traceback.format_exc()}')
    #     raise
async def trade_stream_test(trade):
    trade_received_time = datetime.now(ZoneInfo(TIMEZONE)).strftime(DATE_FMT)
    print('test trade update', trade_received_time, trade)
    # try:
    #     print('test trade update', trade_received_time, trade)
    # except:
    #     print(f'\nException in trade_stream!!!')
    #     print(f'{traceback.format_exc()}')
    #     raise
async def my_trades_stream_test(trade):
    update_received_time = datetime.now(ZoneInfo(TIMEZONE)).strftime(DATE_FMT)
    print('test my trade update', update_received_time, trade)
    # try:
    #     print('test trade update', update_received_time, trade)
    # except:
    #     print(f'\nException in my_trade_stream!!!')
    #     print(f'{traceback.format_exc()}')
    #     raise


if __name__ == "__main__":

    print(1)

    # create quote thread
    # https://alpaca.markets/sdks/python/api_reference/data/stock/live.html#stockdatastream
    price_data_websocket_client = StockDataStream(API_KEY, API_SECRET, feed=DataFeed.IEX)
    price_data_websocket_client.subscribe_quotes(quote_stream_test, *SYMBOLS)
    price_data_websocket_client.subscribe_trades(trade_stream_test, *SYMBOLS)
    price_data_thread = threading.Thread(target=price_data_websocket_client.run)

    print(2)

    # create trade thread
    # https://alpaca.markets/sdks/python/api_reference/trading/stream.html#alpaca.trading.stream.TradingStream
    my_trades_websocket_client = TradingStream(API_KEY, API_SECRET, paper=not LIVE_TRADING)
    my_trades_websocket_client.subscribe_trade_updates(my_trades_stream_test)
    my_trades_thread = threading.Thread(target=my_trades_websocket_client.run)

    print(3)

    # start the threads
    price_data_thread.start()
    print(4)
    my_trades_thread.start()
    print(5)

    # keep main thread running unless manual intervention from terminal (Ctrl + C)
    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            print("\nshutting down...")
            break

    print(6)

    # unsubscribe from quotes and close quotes and trades websocket streams
    # NOTE: if TradingStream had an unsubscribe method, then trade_thread.join() could run instantly (as it is it takes 5 seconds)
    price_data_websocket_client.unsubscribe_quotes(*SYMBOLS)
    price_data_websocket_client.unsubscribe_trades(*SYMBOLS)
    async def stop_streams():
        await price_data_websocket_client.stop_ws()
        await my_trades_websocket_client.stop_ws()
    asyncio.run(stop_streams())

    print(7)

    # join threads back into the main thread once they
    # finish their current *_stream() call
    price_data_thread.join()
    print(8)
    my_trades_thread.join()
    print(9)

    print('trading bot stopped\n')

