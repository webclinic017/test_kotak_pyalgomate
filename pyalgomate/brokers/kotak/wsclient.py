"""
.. moduleauthor:: Nagaraju Gunda
"""

import threading
import queue
import logging
import datetime
import pytz

from pyalgotrade import bar

logger = logging.getLogger(__name__)
logger.propagate = False


class SubscribeEvent(object):
    # t	tk	‘tk’ represents touchline acknowledgement
    # e	NSE, BSE, NFO ..	Exchange name
    # tk	22	Scrip Token
    # pp	2 for NSE, BSE & 4 for CDS USDINR	Price precision
    # ts		Trading Symbol
    # ti		Tick size
    # ls		Lot size
    # lp		LTP
    # pc		Percentage change
    # v		volume
    # o		Open price
    # h		High price
    # l		Low price
    # c		Close price
    # ap		Average trade price
    # oi		Open interest
    # poi		Previous day closing Open Interest
    # toi		Total open interest for underlying
    # bq1		Best Buy Quantity 1
    # bp1		Best Buy Price 1
    # sq1		Best Sell Quantity 1
    # sp1		Best Sell Price 1

    def __init__(self, eventDict):
        self.__eventDict = eventDict
        self.__datetime = None

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def tradingSymbol(self):
        return self.__eventDict["ts"]

    @property
    def dateTime(self):
        if self.__datetime is None:
            ftdm_str = self.__eventDict.get('ftdm')
            if ftdm_str is not None:
                self.__datetime = datetime.datetime.strptime(
                    ftdm_str, '%d/%m/%Y %H:%M:%S')
            else:
                self.__datetime = datetime.datetime.now()

        return self.__datetime

        return self.__datetime

    @dateTime.setter
    def dateTime(self, value):
        self.__datetime = value

    @property
    def tickDateTime(self):
        fdtm_str = self.__eventDict.get('fdtm')
        if fdtm_str is not None:
            return datetime.datetime.strptime(fdtm_str, '%d/%m/%Y %H:%M:%S')
        else:
            return datetime.datetime.now()

    @property
    def price(self): return float(self.__eventDict.get('ltp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime())

    @property
    def instrument(self): return f"{self.tradingSymbol}"

    def TradeBar(self):
        open = high = low = close = self.price

        return bar.BasicBar(self.dateTime,
                            open,
                            high,
                            low,
                            close,
                            self.volume,
                            None,
                            bar.Frequency.TRADE,
                            {
                                "Instrument": self.instrument,
                                "Open Interest": self.openInterest,
                                "Date/Time": self.tickDateTime
                            })


class WebSocketClient:
    """
    This websocket client class is designed to be running in a separate thread and for that reason
    events are pushed into a queue.
    """

    class Event:
        DISCONNECTED = 1
        TRADE = 2
        ORDER_BOOK_UPDATE = 3

    def __init__(self, queue, api, tokenMappings):
        assert len(tokenMappings), "Missing subscriptions"
        self.__queue = queue
        self.__api = api
        self.__tokenMappings = tokenMappings
        self.__pending_subscriptions = list(tokenMappings)
        self.__connected = False
        self.__initialized = threading.Event()
        self.__currentDateTime = None

    def startClient(self):
        self.__api.on_message = self.onQuoteUpdate
        self.__api.on_error = self.onError
        self.__api.on_close = self.onClosed
        self.__api.on_open = self.onOpened
        self.__api.subscribe(self.__pending_subscriptions)
        # start_websocket(order_update_callback=self.onOrderBookUpdate,
        #                            subscribe_callback=self.onQuoteUpdate,
        #                            socket_open_callback=self.onOpened,
        #                            socket_close_callback=self.onClosed,
        #                            socket_error_callback=self.onError)

    def stopClient(self):
        try:
            if self.__connected:
                self.close()
        except Exception as e:
            logger.error("Failed to close connection: %s" % e)

    def setInitialized(self):
        assert self.isConnected()
        self.__initialized.set()

    def waitInitialized(self, timeout):
        logger.info(
            f"Waiting for WebSocketClient waitInitialized with timeout of {timeout}")
        return self.__initialized.wait(timeout)

    def isConnected(self):
        return self.__connected

    def onOpened(self):
        self.__connected = True

        # for channel in self.__pending_subscriptions:
        #     logger.info("Subscribing to channel %s." % channel)
        #     self.__api.subscribe(channel)

    def onClosed(self):
        if self.__connected:
            self.__connected = False

        logger.info("Websocket disconnected")
        # self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onError(self, exception):
        import traceback
        # Get the traceback information
        tb_info = traceback.format_exc()

        # Log the error along with traceback
        logger.error("Error: %s\n%s" % (exception, tb_info))

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onTrade(self, trade):
        if trade.getPrice() > 0:
            self.__queue.put((WebSocketClient.Event.TRADE, trade))

    def onQuoteUpdate(self, message):
        logger.debug(message)
# check
        field = message[0].get("tk")
        # message[0]["ts"] = self.__tokenMappings[f"{message[0]['tk']}"]
        # t='tk' is sent once on subscription for each instrument.
        # this will have all the fields with the most recent value thereon t='tf' is sent for fields that have changed.
        subscribeEvent = SubscribeEvent(message[0])

        # if field not in ["ts", "tk"]:
        #     self.onUnknownEvent(subscribeEvent)
        #     return

        if field == "tk":
            logger.info(f"success with {field}")
            self.__onSubscriptionSucceeded(subscribeEvent)
            # return

        # if subscribeEvent.openInterest  > 0:
        #     print(f'{subscribeEvent.instrument} OI <{subscribeEvent.openInterest}>')

        # dateTime = subscribeEvent.dateTime
        # instrument = subscribeEvent.instrument
        # if self.__currentDateTime is not None and dateTime <= self.__currentDateTime:
        #     logger.debug(f"Current date time <{self.__currentDateTime}> for <{instrument}> is higher/equal than tick date time <{dateTime}>. Modifying tick time!")
        #     subscribeEvent.dateTime = datetime.datetime.now().replace(microsecond=0)
        # self.__currentDateTime = subscribeEvent.dateTime
        subscribeEvent.dateTime = datetime.datetime.now()
        self.onTrade(subscribeEvent.TradeBar())

    def onOrderBookUpdate(self, message):
        hello = True
        # orderBookUpdate = message
        # self.__queue.put(
        #     (WebSocketClient.Event.ORDER_BOOK_UPDATE, orderBookUpdate))

    def __onSubscriptionSucceeded(self, event):
        logger.info(
            f"Subscription succeeded for <{event.tradingSymbol}>")

        self.__pending_subscriptions.remove(
            f"{event.scriptToken}")

        if not self.__pending_subscriptions:
            self.setInitialized()


class WebSocketClientThreadBase(threading.Thread):
    def __init__(self, wsCls, *args, **kwargs):
        super(WebSocketClientThreadBase, self).__init__()
        self.__queue = queue.Queue()
        self.__wsClient = None
        self.__wsCls = wsCls
        self.__args = args
        self.__kwargs = kwargs

    def getQueue(self):
        return self.__queue

    def waitInitialized(self, timeout):
        return self.__wsClient is not None and self.__wsClient.waitInitialized(timeout)

    def run(self):
        # We create the WebSocketClient right in the thread, instead of doing so in the constructor,
        # because it has thread affinity.
        try:
            self.__wsClient = self.__wsCls(
                self.__queue, *self.__args, **self.__kwargs)
            logger.debug("Running websocket client")
            self.__wsClient.startClient()
        except Exception as e:
            logger.exception("Unhandled exception %s" % e)
            self.__wsClient.stopClient()

    def stop(self):
        try:
            if self.__wsClient is not None:
                logger.debug("Stopping websocket client")
                self.__wsClient.stopClient()
        except Exception as e:
            logger.error("Error stopping websocket client: %s" % e)


class WebSocketClientThread(WebSocketClientThreadBase):
    """
    This thread class is responsible for running a WebSocketClient.
    """

    def __init__(self, api, tokenMappings):
        super(WebSocketClientThread, self).__init__(
            WebSocketClient, api, tokenMappings)
