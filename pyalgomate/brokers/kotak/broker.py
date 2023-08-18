"""
.. moduleauthor:: Nagaraju Gunda
"""

import threading
import time
import logging
import datetime
import six
import calendar
import re
import pandas as pd
import csv
import requests
import os
from pyalgotrade import broker
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.strategies import OptionContract
from neo_api_client import NeoAPI
logger = logging.getLogger(__file__)
logger.propagate = False

status_mapping = {
    "rejected": "REJECTED",
    "cancelled": "CANCELED",
    "complete": "COMPLETE",
    "traded": "COMPLETE"}


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    symbol = 'BANKNIFTY'
    if 'BANKNIFTY' in underlyingInstrument:
        symbol = 'BANKNIFTY'
    expiry_date = datetime.strptime(expiry, '%d%b%Y')
    expiry_month = str(int(expiry_date.strftime('%m')))
    if expiry_month.startswith('0'):
        expiry_month = expiry_month[1:]
    expiry_day = expiry_date.strftime('%d')
    return f"{symbol}23{expiry_month}{expiry_day}{strikePrice}{callOrPut}"


def getOptionSymbols(underlyingInstrument, expiry, ltp, count):
    ltp = int(float(ltp) / 100) * 100
    logger.info(f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count+1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp + (n * 100), 'C'))

    for n in range(-count, count+1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp - (n * 100), 'P'))

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols


def getTokenMappings(api, exchangeSymbols):
    tokenMappings = {}    # Fetch file URL from client.scrip_master()
    response = api.scrip_master()
    csv_urls = response['filesPaths']
    x = exchangeSymbols

    # Find the URL for 'nse_fo' CSV file
    nse_fo_csv_url = next((url for url in csv_urls if 'nse_fo' in url), None)
    if nse_fo_csv_url is None:
        print("CSV file URL not found for 'nse_fo'")
        return []

    filename = 'nse_fo.csv'  # Local filename to save the downloaded CSV

    # Download CSV file
    response = requests.get(nse_fo_csv_url)
    with open(filename, 'wb') as file:
        file.write(response.content)

    # Perform search operation for each value in x
    column_name = 'pScripRefKey'
    symbol_column_name = 'pSymbol'
    output_array = []

    with open(filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        for value in x:
            found_symbol = None
            for row in csv_reader:
                if row[column_name] == value:
                    found_symbol = row[symbol_column_name]
                    break
            output_array.append(found_symbol)
            # Reset the file reader to the beginning for the next search
            file.seek(0)

    # If symbols not found, modify x with the first three letters of the month
    if all(symbol is None for symbol in output_array):
        now = datetime.now()
        # First three letters of the current month
        month_abbrev = now.strftime('%b').upper()
        modified_x = tuple(
            f"{symbol[:3]}{month_abbrev}{symbol[4:]}" for symbol in x)
        x = modified_x

        # Perform search operation again for modified x
        output_array = []
        with open(filename, 'r') as file:
            csv_reader = csv.DictReader(file)
            for value in x:
                found_symbol = None
                for row in csv_reader:
                    if row[column_name] == value:
                        found_symbol = row[symbol_column_name]
                        break
                output_array.append(found_symbol)
                # Reset the file reader to the beginning for the next search
                file.seek(0)

    instrument_tokens = output_array
    # Delete the downloaded file
    os.remove(filename)
    inst_tokens = [{"instrument_token": "26037", "exchange_segment": "nse_cm"}]
    # Function to populate inst_tokens list

    def populate_inst_tokens(instrument_tokens, inst_tokens):
        inst_tokens.clear()  # Clear the initial entry
        for token in instrument_tokens:
            inst_tokens.append(
                {"instrument_token": token, "exchange_segment": "nse_fo"})

    # Call the function to populate inst_tokens
    populate_inst_tokens(instrument_tokens, inst_tokens)
    return inst_tokens

# defer


# def getHistoricalData(api, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
#     startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
#     splitStrings = exchangeSymbol.split('|')
#     exchange = splitStrings[0]

#     logger.info(
#         f'Retrieving {interval} timeframe historical data for {exchangeSymbol}')
#     ret = api.get_time_price_series(exchange=exchange, token=getFinvasiaToken(
#         api, exchangeSymbol), starttime=startTime.timestamp(), interval=interval)
#     if ret != None:
#         df = pd.DataFrame(
#             ret)[['time', 'into', 'inth', 'intl', 'intc', 'v', 'oi']]
#         df = df.rename(columns={'time': 'Date/Time', 'into': 'Open', 'inth': 'High',
#                                 'intl': 'Low', 'intc': 'Close', 'v': 'Volume', 'oi': 'Open Interest'})
#         df['Ticker'] = exchangeSymbol
#         df[['Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']] = df[[
#             'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']].astype(float)
#         df['Date/Time'] = pd.to_datetime(df['Date/Time'],
#                                          format="%d-%m-%Y %H:%M:%S")
#         df = df[['Ticker', 'Date/Time', 'Open', 'High',
#                  'Low', 'Close', 'Volume', 'Open Interest']]
#         df = df.sort_values('Date/Time')
#         logger.info(f'Retrieved {df.shape[0]} rows of historical data')
#         return df
#     else:
#         return pd.DataFrame(columns=['Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'])


class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker.
    """

    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)

        self.__api = barFeed.getApi()
    # defer

    # def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    #     return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = 'BANKNIFTY'
        if 'BANKNIFTY' in underlyingInstrument:
            symbol = 'BANKNIFTY'
        expiry_date = datetime.strptime(expiry, '%d%b%Y')
        expiry_month = str(int(expiry_date.strftime('%m')))
        if expiry_month.startswith('0'):
            expiry_month = expiry_month[1:]
        expiry_day = expiry_date.strftime('%d')
        return f"{symbol}23{expiry_month}{expiry_day}{strikePrice}{callOrPut}"

    def getOptionSymbols(underlyingInstrument, expiry, ltp, count):
        ltp = int(float(ltp) / 100) * 100
        logger.info(
            f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
        optionSymbols = []
        for n in range(-count, count+1):
            optionSymbols.append(getOptionSymbol(
                underlyingInstrument, expiry, ltp + (n * 100), 'C'))

        for n in range(-count, count+1):
            optionSymbols.append(getOptionSymbol(
                underlyingInstrument, expiry, ltp - (n * 100), 'P'))

        logger.info("Options symbols are " + ",".join(optionSymbols))
        return optionSymbols

    def getOptionContract(symbol) -> OptionContract:
        m = re.match(
            r"([A-Z\|]+)(\d{2})([A-Z]{1}[a-z]{2})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            return None

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)

        option_type = "c" if m.group(5) == "CE" else "p"

        return OptionContract(symbol, int(m.group(6)), expiry, option_type)

    pass

    # get_order_book
    # Response data will be in json Array of objects with below fields in case of success.

    # Json Fields	Possible value	Description
    # stat	        Ok or Not_Ok	Order book success or failure indication.
    # exch		                    Exchange Segment
    # tsym		                    Trading symbol / contract on which order is placed.
    # norenordno		            Noren Order Number
    # prc		                    Order Price
    # qty		                    Order Quantity
    # prd		                    Display product alias name, using prarr returned in user details.
    # status
    # trantype	    B / S       	Transaction type of the order
    # prctyp	    LMT / MKT   	Price type
    # fillshares	                Total Traded Quantity of this order
    # avgprc		                Average trade price of total traded quantity
    # rejreason		                If order is rejected, reason in text form
    # exchordid		                Exchange Order Number
    # cancelqty		                Canceled quantity for order which is in status cancelled.
    # remarks		                Any message Entered during order entry.
    # dscqty		                Order disclosed quantity.
    # trgprc		                Order trigger price
    # ret           DAY / IOC / EOS	Order validity
    # uid
    # actid
    # bpprc		                    Book Profit Price applicable only if product is selected as B (Bracket order )
    # blprc		                    Book loss Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # trailprc	                    Trailing Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # amo		                    Yes / No
    # pp		                    Price precision
    # ti		                    Tick size
    # ls		                    Lot size
    # token		                    Contract Token
    # norentm
    # ordenttm
    # exch_tm
    # snoordt		                0 for profit leg and 1 for stoploss leg
    # snonum		                This field will be present for product H and B; and only if it is profit/sl order.

    # Response data will be in json format with below fields in case of failure:

    # Json Fields	Possible value	Description
    # stat	        Not_Ok	        Order book failure indication.
    # request_time		            Response received time.
    # emsg		                    Error message

    # single_order_history

    # Json Fields	Possible value	Description
    # stat	        Ok or Not_Ok	Order book success or failure indication.
    # exch		                    Exchange Segment
    # tsym		                    Trading symbol / contract on which order is placed.
    # norenordno		            Noren Order Number
    # prc		                    Order Price
    # qty		                    Order Quantity
    # prd		                    Display product alias name, using prarr returned in user details.
    # status
    # rpt		                    (fill/complete etc)
    # trantype	    B / S	        Transaction type of the order
    # prctyp	    LMT / MKT	    Price type
    # fillshares	                Total Traded Quantity of this order
    # avgprc		                Average trade price of total traded quantity
    # rejreason		                If order is rejected, reason in text form
    # exchordid		                Exchange Order Number
    # cancelqty		                Canceled quantity for order which is in status cancelled.
    # remarks		                Any message Entered during order entry.
    # dscqty		                Order disclosed quantity.
    # trgprc		                Order trigger price
    # ret	        DAY / IOC / EOS	Order validity
    # uid
    # actid
    # bpprc		                    Book Profit Price applicable only if product is selected as B (Bracket order )
    # blprc		                    Book loss Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # trailprc	                    	Trailing Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # amo		                    Yes / No
    # pp		                    Price precision
    # ti		                    Tick size
    # ls		                    Lot size
    # token		                    Contract Token
    # norentm
    # ordenttm
    # exch_tm
    #
    # Response data will be in json format with below fields in case of failure:

    # Json Fields	Possible value	Description
    # stat	        Not_Ok	        Order book failure indication.
    # request_time		            Response received time.
    # emsg		                    Error message


class TradeEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict

    def getId(self):
        return self.__eventDict.get('nOrdNo', None)

    def getStatus(self):
        return self.__eventDict.get('ordSt', None)

    def getRejectedReason(self):
        return self.__eventDict.get('rejRsn', None)

    def getAvgFilledPrice(self):
        return float(self.__eventDict.get('avgPrc', 0.0))

    def getTotalFilledQuantity(self):
        return float(self.__eventDict.get('fldQty', 0.0))

    def getDateTime(self):
        return datetime.datetime.strptime(self.__eventDict['flDtTm'], '%d-%b-%Y %H:%M:%S') if self.__eventDict.get('flDtTm', None) is not None else None

# def getOrderStatus(orderId):
#     orderBook = api.get_order_book()
#     for order in orderBook:
#         if order['norenordno'] == orderId and order['status'] == 'REJECTED':
#             return item['rejreason']
#         elif order['norenordno'] == orderId and order['status'] == 'OPEN':
#             return item['status']
#         elif order['norenordno'] == orderId and order['status'] == 'COMPLETE':
#             print(f'{orderId} successfully placed')
#             return item['status']
#     print(f'{orderId} not found in the order book')
#     return None

# def getFillPrice(orderId):
#     tradeBook = api.get_trade_book()
#     if tradeBook is None:
#         print('No order placed for the day')
#     else:
#         for trade in tradeBook:
#             if trade['norenordno'] == orderId:
#                 return trade['flprc']
#     return None


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 2

    # Events
    ON_USER_TRADE = 1

    def __init__(self, liveBroker: broker.Broker):
        super(TradeMonitor, self).__init__()
        self.__api = liveBroker.getApi()
        self.__broker = liveBroker
        self.__queue = six.moves.queue.Queue()
        self.__stop = False

    def _getNewTrades(self):
        ret = []
        # a = self.__api.order_report()
        # activeOrderIds = [i['nOrdNo'] for i in a['data']]
        activeOrderIds = [order.getId()
                          for order in self.__broker.getActiveOrders().copy()]
        for orderId in activeOrderIds:
            orderHistories = self.__api.order_history(
                order_id=orderId)
            if orderHistories is None:
                logger.info(
                    f'Order history not found for order id {orderId}')
                continue

            if 'data' not in orderHistories:
                continue

            if 'data' not in orderHistories['data']:
                continue

            for orderHistory in orderHistories['data']['data']:
                status = status_mapping.get(
                    orderHistory['ordSt'], orderHistory['ordSt'])
                # if orderHistory['stat'] == 'Not_Ok':
                #     errorMsg = orderHistory['rejRsn']
                #     logger.error(
                #         f'Fetching order history for {orderId} failed with with reason {errorMsg}')
                #     continue
                # ["rejected", "cancelled", "complete", "traded"]
                if status in ['put order req received', 'validation pending', 'open pending', 'OPEN', 'PENDING', 'TRIGGER_PENDING']:
                    continue
                # ["rejected", "cancelled", "complete", "traded"]

                # Your existing code
                elif orderHistory['ordSt'] in ["rejected", "cancelled", "complete", "traded"]:
                    orderHistory['ordSt'] = status
                    ret.append(TradeEvent(orderHistory))  # event
                else:
                    logger.error(
                        f'Unknown trade status {orderHistory.get("vendorCode", None)}')

        # Sort by time, so older trades first.
        return sorted(ret, key=lambda t: t.getDateTime())

    def getQueue(self):
        return self.__queue

    def start(self):
        trades = self._getNewTrades()
        if len(trades):
            logger.info(
                f'Last trade found at {trades[-1].getDateTime()}. Order id {trades[-1].getId()}')

        super(TradeMonitor, self).start()

    def run(self):
        while not self.__stop:
            try:
                trades = self._getNewTrades()
                if len(trades):
                    logger.info(f'{len(trades)} new trade/s found')
                    self.__queue.put((TradeMonitor.ON_USER_TRADE, trades))
            except Exception as e:
                logger.critical(
                    "Error retrieving user transactions", exc_info=e)

            time.sleep(TradeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__stop = True


class OrderResponse(object):

    # Sample Success Response: { "request_time": "10:48:03 20-05-2020", "stat": "Ok", "norenordno": "20052000000017" }
    # Sample Error Response : { "stat": "Not_Ok", "request_time": "20:40:01 19-05-2020", "emsg": "Error Occurred : 2 "invalid input"" }

    def __init__(self, dict):
        self.__dict = dict

    def getId(self):
        return self.__dict["nOrdNo"]

    def getDateTime(self):
        return datetime.datetime.now()

    def getStat(self):
        return self.__dict.get("stat", None)

    def getErrorMessage(self):
        return self.__dict.get("Error", None)


class LiveBroker(broker.Broker):
    """A Finvasia live broker.

    :param api: Logged in api object.
    :type api: ShoonyaApi.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
        * API access permissions should include:

          * Account balance
          * Open orders
          * Buy limit order
          * User transactions
          * Cancel order
          * Sell limit order
    """

    QUEUE_TIMEOUT = 0.01

    def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = 'BANKNIFTY'
        if 'BANKNIFTY' in underlyingInstrument:
            symbol = 'BANKNIFTY'
        expiry_date = datetime.strptime(expiry, '%d%b%Y')
        expiry_month = str(int(expiry_date.strftime('%m')))
        if expiry_month.startswith('0'):
            expiry_month = expiry_month[1:]
        expiry_day = expiry_date.strftime('%d')
        return f"{symbol}23{expiry_month}{expiry_day}{strikePrice}{callOrPut}"

    def getOptionSymbols(underlyingInstrument, expiry, ltp, count):
        ltp = int(float(ltp) / 100) * 100
        logger.info(
            f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
        optionSymbols = []
        for n in range(-count, count+1):
            optionSymbols.append(getOptionSymbol(
                underlyingInstrument, expiry, ltp + (n * 100), 'C'))

        for n in range(-count, count+1):
            optionSymbols.append(getOptionSymbol(
                underlyingInstrument, expiry, ltp - (n * 100), 'P'))

        logger.info("Options symbols are " + ",".join(optionSymbols))
        return optionSymbols

    def getOptionContract(symbol) -> OptionContract:
        m = re.match(
            r"([A-Z\|]+)(\d{2})([A-Z]{1}[a-z]{2})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            return None

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)

        option_type = "c" if m.group(5) == "CE" else "p"

        return OptionContract(symbol, int(m.group(6)), expiry, option_type)

    # def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    #     return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: NeoAPI):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__api = api
        self.__tradeMonitor = TradeMonitor(self)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}

    def getApi(self):
        return self.__api

    def getInstrumentTraits(self, instrument):
        return QuantityTraits()

    def _registerOrder(self, order):
        assert (order.getId() not in self.__activeOrders)
        assert (order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert (order.getId() in self.__activeOrders)
        assert (order.getId() is not None)
        del self.__activeOrders[order.getId()]

    def refreshAccountBalance(self):
        self.__stop = True  # Stop running in case of errors.

    def refreshOpenOrders(self):
        return
        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving open orders.")
        openOrders = None  # self.__api.getOpenOrders()
        # for openOrder in openOrders:
        #     self._registerOrder(build_order_from_open_order(
        #         openOrder, self.getInstrumentTraits(common.btc_symbol)))

        logger.info("%d open order/s found" % (len(openOrders)))
        self.__stop = False  # No errors. Keep running.

    def _startTradeMonitor(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Initializing trade monitor.")
        self.__tradeMonitor.start()
        self.__stop = False  # No errors. Keep running.

    def _onTrade(self, order, trade):
        if trade.getStatus() == 'REJECTED' or trade.getStatus() == 'CANCELED':
            if trade.getRejectedReason() is not None:
                logger.error(
                    f'Order {trade.getId()} rejected with reason {trade.getRejectedReason()}')
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(broker.OrderEvent(
                order, broker.OrderEvent.Type.CANCELED, None))
        # elif trade.getStatus() == 'OPEN':
        #     order.switchState(broker.Order.State.ACCEPTED)
        #     self.notifyOrderEvent(broker.OrderEvent(
        #         order, broker.OrderEvent.Type.ACCEPTED, None))
        elif trade.getStatus() == 'COMPLETE':
            fee = 0
            orderExecutionInfo = broker.OrderExecutionInfo(
                trade.getAvgFilledPrice(), trade.getTotalFilledQuantity() - order.getFilled(), fee, trade.getDateTime())
            order.addExecutionInfo(orderExecutionInfo)
            if not order.isActive():
                self._unregisterOrder(order)
            # Notify that the order was updated.
            if order.isFilled():
                eventType = broker.OrderEvent.Type.FILLED
            else:
                eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
            self.notifyOrderEvent(broker.OrderEvent(
                order, eventType, orderExecutionInfo))
        else:
            logger.error(f'Unknown order status {trade.getStatus()}')

    def _onUserTrades(self, trades):
        for trade in trades:
            order = self.__activeOrders.get(trade.getId())
            if order is not None:
                self._onTrade(order, trade)
            else:
                logger.info(
                    f"Trade {trade.getId()} refered to order that is not active")

    # BEGIN observer.Subject interface
    def start(self):
        super(LiveBroker, self).start()
        self.refreshAccountBalance()
        self.refreshOpenOrders()
        self._startTradeMonitor()

    def stop(self):
        self.__stop = True
        logger.info("Shutting down trade monitor.")
        self.__tradeMonitor.stop()

    def join(self):
        if self.__tradeMonitor.isAlive():
            self.__tradeMonitor.join()

    def eof(self):
        return self.__stop

    def dispatch(self):
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = list(self.__activeOrders.values())
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(
                    order, broker.OrderEvent.Type.ACCEPTED, None))

        # Dispatch events from the trade monitor.
        try:
            eventType, eventData = self.__tradeMonitor.getQueue().get(
                True, LiveBroker.QUEUE_TIMEOUT)

            if eventType == TradeMonitor.ON_USER_TRADE:
                self._onUserTrades(eventData)
            else:
                logger.error(
                    "Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except six.moves.queue.Empty:
            pass

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # END observer.Subject interface

    # BEGIN broker.Broker interface

    def getCash(self, includeShort=True):
        return self.__cash

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        return self.__shares

    def getActiveOrders(self, instrument=None):
        return list(self.__activeOrders.values())

    # Place a Limit order as follows
#     api.place_order(buy_or_sell='B', product_type='C',
#                         exchange='NSE', tradingsymbol='INFY-EQ',
#                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
#                         retention='DAY', remarks='my_order_001')
# Place a Market Order as follows
#     api.place_order(buy_or_sell='B', product_type='C',
#                         exchange='NSE', tradingsymbol='INFY-EQ',
#                         quantity=1, discloseqty=0,price_type='MKT', price=0, trigger_price=None,
#                         retention='DAY', remarks='my_order_001')
# Place a StopLoss Order as follows
#     api.place_order(buy_or_sell='B', product_type='C',
#                         exchange='NSE', tradingsymbol='INFY-EQ',
#                         quantity=1, discloseqty=0,price_type='SL-LMT', price=1500, trigger_price=1450,
#                         retention='DAY', remarks='my_order_001')
# Place a Cover Order as follows
#     api.place_order(buy_or_sell='B', product_type='H',
#                         exchange='NSE', tradingsymbol='INFY-EQ',
#                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
#                         retention='DAY', remarks='my_order_001', bookloss_price = 1490)
# Place a Bracket Order as follows
#     api.place_order(buy_or_sell='B', product_type='B',
#                         exchange='NSE', tradingsymbol='INFY-EQ',
#                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
#                         retention='DAY', remarks='my_order_001', bookloss_price = 1490, bookprofit_price = 1510)
# Modify Order
# Modify a New Order by providing the OrderNumber
#     api.modify_order(exchange='NSE', tradingsymbol='INFY-EQ', orderno=orderno,
#                                    newquantity=2, newprice_type='LMT', newprice=1505)
# Cancel Order
# Cancel a New Order by providing the Order Number
#     api.cancel_order(orderno=orderno)

    def __placeOrder(self, buyOrSell, productType, exchange, symbol, quantity, price, priceType, triggerPrice, retention, remarks):
        try:
            orderResponse = self.__api.place_order(exchange_segment=exchange, product=productType, price=str(price), order_type=priceType,
                                                   quantity=str(quantity), validity=retention, trading_symbol=symbol,
                                                   transaction_type=buyOrSell, amo="NO", disclosed_quantity="0", market_protection="0", pf="N",
                                                   trigger_price=str(triggerPrice), tag=None)
            # orderResponse = self.__api.place_order(buy_or_sell=buyOrSell, product_type=productType,
            #                                        exchange=exchange, tradingsymbol=symbol,
            #                                        quantity=quantity, discloseqty=0, price_type=priceType,
            #                                        price=price, trigger_price=triggerPrice,
            #                                        retention=retention, remarks=remarks)
        except Exception as e:
            raise Exception(e)

        if orderResponse is None:
            raise Exception('place_order returned None')

        ret = OrderResponse(orderResponse)

        if 'stat' not in orderResponse or orderResponse['stat'] != "Ok":
            raise Exception(ret.getErrorMessage())

        return ret

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            buyOrSell = 'B' if order.isBuy() else 'S'
            # "C" For CNC, "M" FOR NRML, "I" FOR MIS, "B" FOR BRACKET ORDER, "H" FOR COVER ORDER
            productType = 'NRML'
            exchange = 'nse_fo'
            symbol = order.getInstrument()
            quantity = order.getQuantity()
            price = order.getLimitPrice() if order.getType() in [
                broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT] else 0
            stopPrice = order.getStopPrice() if order.getType() in [
                broker.Order.Type.STOP_LIMIT] else 0
            priceType = {
                # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
                broker.Order.Type.MARKET: 'MKT',
                broker.Order.Type.LIMIT: 'L',
                broker.Order.Type.STOP_LIMIT: 'SL',
                broker.Order.Type.STOP: 'SL-M'
            }.get(order.getType())
            retention = 'DAY'  # DAY, IOC, DAY, IOC, GTC, EOS

            logger.info(
                f'Placing {priceType} {"Buy" if order.isBuy() else "Sell"} order for {order.getInstrument()} with {quantity} quantity')
            try:
                kotakOrder = self.__placeOrder(buyOrSell,
                                               productType,
                                               exchange,
                                               symbol,
                                               quantity,
                                               price,
                                               priceType,
                                               stopPrice,
                                               retention,
                                               None)
            except Exception as e:
                logger.critical(
                    f'Could not place order for {symbol}. Reason: {e}')
                return

            try:
                logger.info(
                    f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} order {kotakOrder.getId()} at {kotakOrder.getDateTime()}')
                order.setSubmitted(kotakOrder.getId(),
                                   kotakOrder.getDateTime())
            except Exception as e:
                logger.critical(
                    f'Could not place order for {symbol}. Reason: {e}')
                return

            self._registerOrder(order)
            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def _createOrder(self, orderType, action, instrument, quantity, price, stopPrice):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
        }.get(action, None)

        if action is None:
            raise Exception("Only BUY/SELL orders are supported")

        if orderType == broker.MarketOrder:
            return broker.MarketOrder(action, instrument, quantity, False, self.getInstrumentTraits(instrument))
        elif orderType == broker.LimitOrder:
            return broker.LimitOrder(action, instrument, price, quantity, self.getInstrumentTraits(instrument))
        elif orderType == broker.StopOrder:
            return broker.StopOrder(action, instrument, stopPrice, quantity, self.getInstrumentTraits(instrument))
        elif orderType == broker.StopLimitOrder:
            return broker.StopLimitOrder(action, instrument, stopPrice, price, quantity, self.getInstrumentTraits(instrument))

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(broker.MarketOrder, action, instrument, quantity, None, None)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice, None)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return self._createOrder(broker.StopOrder, action, instrument, quantity, None, stopPrice)

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return self._createOrder(broker.StopLimitOrder, action, instrument, quantity, limitPrice, stopPrice)

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.__api.cancel_order(orderno=order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Update cash and shares.
        self.refreshAccountBalance()

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(
            order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
