import json, os, time
import pandas as pd
from datetime import datetime
from pytz import timezone
import multiprocessing as mp
from alpaca.data.live.stock import StockDataStream
from alpaca.data.enums import DataFeed


'''

    description:

        get realtime bid/ask spread data with a persistent connection:

                "The get_stock_latest_quote() function is a good way to get the latest quote data for a specific stock. However, if you need real-time data and you're making requests frequently (like every 5 seconds), using the StockDataStream class might be a more efficient approach. The StockDataStream class allows you to subscribe to real-time data via WebSockets. This means that instead of making a new request every few seconds, you establish a persistent connection and receive updates as they happen. This can provide better performance and more timely data."
                    Kapa AI, chatbot on Alpaca Slack Q&A Channel

    sources:

        https://alpaca.markets/sdks/python/api_reference/data/stock/live.html
        https://app.slack.com/client/TD8AD6C1J/CEL9HCSN4

        TODO: maybe use this library instead of multiprocessing
            https://pythonhosted.org/Pebble/#concurrent-functions
                recommendation source: https://stackoverflow.com/questions/32053618/how-to-to-terminate-process-using-pythons-multiprocessing

        convo with chatgpt about library choice

            asyncio and threading are both concurrency mechanisms in Python, but they have different approaches to handling concurrent tasks. Here's a comparison of asyncio and threading along with their use cases:
            asyncio:

                Concurrency Model:
                    Asynchronous (Event-driven): asyncio is designed for asynchronous programming using the event-driven concurrency model. It allows you to write non-blocking code using coroutines.

                Execution Model:
                    Single-threaded Event Loop: asyncio typically runs in a single-threaded event loop. Multiple tasks can be interleaved within the same thread using coroutines.

                Use Cases:
                    I/O-bound Operations: asyncio is well-suited for I/O-bound tasks, such as network operations or file I/O, where waiting for external resources doesn't block the entire program.
                    Highly Concurrent Servers: It is commonly used in building high-concurrency network servers and applications.

                Pros:
                    Scalability: Well-suited for handling a large number of concurrent I/O-bound operations efficiently.
                    No Thread-Related Overhead: Avoids the overhead associated with threading, making it more lightweight.

                Cons:
                    CPU-bound Tasks: Not the best choice for CPU-bound tasks, as it doesn't provide true parallelism.

            threading:

                Concurrency Model:
                    Thread-based: threading provides a multi-threading model where each thread runs in parallel, potentially on multiple CPU cores.

                Execution Model:
                    Multiple Threads: threading allows multiple threads to run concurrently, each with its own execution path.

                Use Cases:
                    CPU-bound Operations: Suitable for tasks that are CPU-bound and can benefit from parallel execution, as threads run concurrently on multiple cores.
                    Parallelism: Useful for parallelizing independent tasks to improve overall program performance.

                Pros:
                    Parallel Execution: Well-suited for CPU-bound tasks where parallel execution can provide performance benefits.
                    Broad Compatibility: Works well with existing synchronous code that is not designed for asynchronous execution.

                Cons:
                    GIL (Global Interpreter Lock): In CPython (the default Python implementation), the Global Interpreter Lock can limit the effectiveness of threading for CPU-bound tasks, as it allows only one thread to execute Python bytecode at a time.

            Common Considerations:

                Ease of Use:
                    asyncio: Requires understanding and working with asynchronous programming concepts like coroutines and event loops.
                    threading: More traditional and might be easier for developers familiar with multi-threading concepts.

                Concurrency vs. Parallelism:
                    asyncio: Primarily for concurrency, not true parallelism.
                    threading: Enables true parallelism, especially on multi-core systems.

                Compatibility:
                    asyncio: Best suited for new projects or projects designed for asynchronous programming.
                    threading: Can be used with existing synchronous code but may have limitations due to the Global Interpreter Lock in CPython.

            In summary, asyncio is well-suited for I/O-bound tasks with high concurrency requirements, while threading is more appropriate for CPU-bound tasks that can benefit from parallel execution. The choice between them depends on the nature of the problem you are solving and the type of tasks your application needs to handle. Additionally, the use of other concurrency libraries, such as multiprocessing, is another consideration for certain scenarios.


'''


# Alpaca API Constants
LIVE_TRADING = False
CREDENTIALS_FILEPATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "credentials.json")
with open(CREDENTIALS_FILEPATH) as f:
	creds = json.load(f)
ENDPOINT   = creds['live_trading' if LIVE_TRADING else 'paper_trading']['ENDPOINT']
API_KEY    = creds['live_trading' if LIVE_TRADING else 'paper_trading']['API_KEY_ID']
API_SECRET = creds['live_trading' if LIVE_TRADING else 'paper_trading']['SECRET_KEY']
HEADERS = {
    "accept": "application/json",
    "APCA-API-KEY-ID": API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET,
}
EXCHANGE = 'iex'

INTERVAL = 3 # seconds, guy on Alpaca Slack said you can query every 3 seconds instead of every 5
TICKERS = [
    # "AAPL",
    # "TSLA",
    "LMT",
    "JNJ",
    "CVX",
]
TIMEZONE = 'UTC' # 'EST' # 'EDT'
''' TIMEZONE NOTE:
Eastern Standard Time (EST), when observing standard time (autumn/winter), are five hours behind Coordinated Universal Time (UTC−05:00). Eastern Daylight Time (EDT), when observing daylight saving time (spring/summer), are four hours behind Coordinated Universal Time (UTC−04:00). On the second Sunday in March, at 2:00 a.m. EST, clocks are advanced to 3:00 a.m. EDT leaving a one-hour gap. On the first Sunday in November, at 2:00 a.m. EDT, clocks are moved back to 1:00 a.m. EST, which results in one hour being duplicated.

source: https://en.wikipedia.org/wiki/Eastern_Time_Zone#:~:text=Eastern%20Standard%20Time%20(EST)%2C,UTC%E2%88%9204%3A00).
'''
DATE_FMT = '%Y-%m-%d %H:%M:%S %Z'

wss_client = StockDataStream(API_KEY, API_SECRET, feed=DataFeed.IEX)


quote_filepath = 'quotes.csv'
trades_filepath = 'trades.csv'
open(quote_filepath, 'w').close() # clear file
open(trades_filepath, 'w').close() # clear file
async def quote_data_handler(quote):

    # when quote data changes in any way for any of the listed tickers given to subscribe_quotes
    # this function will return ithat ticker's updated quote as an alpaca quote object
    # alpaca.data.models.quotes.Quote
    # https://alpaca.markets/sdks/python/api_reference/data/models.html#quote

    # when quote data changes in any way for any of the listed tickers given to subscribe_quotes
    # this function will return ithat ticker's updated quote as an alpaca quote object
    # alpaca.data.models.trades.Trade
    # https://alpaca.markets/sdks/python/api_reference/data/models.html#trade
    quote_received_time = datetime.now(timezone(TIMEZONE)).strftime(DATE_FMT)
    df = pd.DataFrame({
        'symbol'              : [quote.symbol], # ticker identifier for the security. TYPE: str
        'timestamp'           : [quote.timestamp], # time of submission of the quote. TYPE: datetime
        'ask_exchange'        : [quote.ask_exchange], # exchange of the quote ask. Defaults to None. TYPE: Optional[str, Exchange]
        'ask_price'           : [quote.ask_price], # asking price of the quote. TYPE: float
        'ask_size'            : [quote.ask_size], # size of the quote ask. TYPE: float
        'bid_exchange'        : [quote.bid_exchange], # exchange of the quote bid. Defaults to None. TYPE: Optional[str, Exchange]
        'bid_price'           : [quote.bid_price], # bid price of the quote. TYPE: float
        'bid_size'            : [quote.bid_size], # size of the quote bid. TYPE: float
        'conditions'          : [quote.conditions], # quote conditions. Defaults to None. TYPE: Optional[Union[List[str], str]]
        'tape'                : [quote.tape], # quote tape. Defaults to None. TYPE: Optional[str]
        'quote_recieved_time' : [quote_received_time],
    }, index=[0])

    # save row to CSV without reading the entire CSV
    if (not os.path.exists(quote_filepath)) or (os.path.getsize(quote_filepath) == 0):
        df.to_csv(quote_filepath, index=False)
    else:
        tmp_filepath = 'tmp.csv'
        df.to_csv(tmp_filepath, index=False, header=False)
        with open(tmp_filepath, 'r') as tmp_file:
            with open(quote_filepath, 'a') as quotes_file:
                new_row = tmp_file.read()
                quotes_file.write(new_row)
        os.remove(tmp_filepath)
    print(f'saved quote for {quote_received_time} to CSV')
async def trade_data_handler(trade):

    # when quote data changes in any way for any of the listed tickers given to subscribe_quotes
    # this function will return ithat ticker's updated quote as an alpaca quote object
    # alpaca.data.models.trades.Trade
    # https://alpaca.markets/sdks/python/api_reference/data/models.html#trade
    trade_received_time = datetime.now(timezone(TIMEZONE)).strftime(DATE_FMT)
    df = pd.DataFrame({
        'symbol'              : [trade.symbol], # ticker identifier for the security. TYPE: str
        'timestamp'           : [trade.timestamp], # time of submission of the trade. TYPE: datetime
        'exchange'            : [trade.exchange], # exchange the trade occurred. TYPE: Optional[Exchange]
        'price'               : [trade.price], # price that the transaction occurred at. TYPE: float
        'size'                : [trade.size], # quantity traded TYPE: float
        'id'                  : [trade.id], # trade ID TYPE: Optional[int]
        'conditions'          : [trade.conditions], # trade conditions. Defaults to None. TYPE: Optional[Union[List[str], str]]
        'tape'                : [trade.tape], # trade tape. Defaults to None. TYPE: Optional[str]
        'trade_recieved_time' : [trade_received_time]
    }, index=[0])

    # save row to CSV without reading the entire CSV
    if (not os.path.exists(trades_filepath)) or (os.path.getsize(trades_filepath) == 0):
        df.to_csv(trades_filepath, index=False)
    else:
        tmp_filepath = 'tmp.csv'
        df.to_csv(tmp_filepath, index=False, header=False)
        with open(tmp_filepath, 'r') as tmp_file:
            with open(trades_filepath, 'a') as trades_file:
                new_row = tmp_file.read()
                trades_file.write(new_row)
        os.remove(tmp_filepath)
    print(f'saved trade for {trade_received_time} to CSV')
def collect_data_in_separate_process():

    wss_client.subscribe_quotes(quote_data_handler, *TICKERS)
    wss_client.subscribe_trades(trade_data_handler, *TICKERS)
    # source to subscribe_quotes and subscribe_trades
    # https://alpaca.markets/sdks/python/api_reference/data/stock/live.html#stockdatastream

    # if data is not collected in a separate thread then
    # the script will be blocked after calling "wss_client.run()"
    # and "print('stream started')" will not run
    print('starting stream')
    wss_client.run()
    print('finished wss_client.run()')
    # NOTE: errors in this thread will print to console and will stop this thread
    # but will not the parent thread. Handle errors in this thread with a try/execpt block
    # source: convo with kapa.ai: https://alpaca-community.slack.com/archives/CEL9HCSN4/p1708615661484309

if __name__ == '__main__':
    print('creating process')
    process = mp.Process(target=collect_data_in_separate_process, daemon=True)
    print('process created')
    process.start()
    print('ran "process.start()"')
    # NOTE: process.start() must be in main function, can't be in script itself (with no indent, like "process = ...")
    # RuntimeError:
    #     An attempt has been made to start a new process before the
    #     current process has finished its bootstrapping phase.

    time.sleep(5)

    # wss_client.unsubscribe_quotes()
    # wss_client.unsubscribe_trades()
    # wss_client.close()

    print('terminating process')
    process.terminate()
    print('process terminated')
    # source: https://stackoverflow.com/questions/32053618/how-to-to-terminate-process-using-pythons-multiprocessing

