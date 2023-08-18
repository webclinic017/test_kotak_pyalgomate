import logging
import datetime
import pandas as pd
import yaml
import pyotp
import glob
import os
import time
import csv
import requests
import pyalgotrade.bar
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

import pyalgomate.utils as utils

from pyalgotrade.strategy import BaseStrategy

from pyalgomate.backtesting import CustomCSVFeed
from pyalgomate.brokers.kotak.broker import BacktestingBroker
from pyalgomate.brokers.kotak.feed import LiveTradeFeed
from pyalgomate.brokers.kotak.broker import PaperTradingBroker, LiveBroker
# from pyalgomate.strategies import BaseOptionsGreeksStrategy
import pyalgomate.brokers.kotak as kotak
from neo_api_client import NeoAPI


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)

x = ['BANKNIFTY2382443800CE']


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
    inst_tokens = [
        {"instrument_token": "257349", "exchange_segment": "mcx_fo"}]
    # Function to populate inst_tokens list

    def populate_inst_tokens(instrument_tokens, inst_tokens):
        inst_tokens.clear()  # Clear the initial entry
        for token in instrument_tokens:
            inst_tokens.append(
                {"instrument_token": token, "exchange_segment": "mcx_fo"})

    # Call the function to populate inst_tokens
    populate_inst_tokens(instrument_tokens, inst_tokens)
    return inst_tokens


class State(object):
    LIVE = 1
    PLACING_ORDERS = 2
    ENTERED = 3
    EXITED = 4


class IntradayData(BaseStrategy):
    def __init__(self, feed, broker):
        super(IntradayData, self).__init__(feed, broker)
        self.resampleBarFeed(
            5 * pyalgotrade.bar.Frequency.MINUTE, self.onResampledBars)
        self.state = State.LIVE
        self.openPositions = {}
        self.Position = None
        self.orderTime = None

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        action = "Buy" if position.getEntryOrder().isBuy() else "Sell"
        self.openPositions[position.getInstrument()] = position.getEntryOrder()
        logger.info(f"{execInfo.getDateTime()} ===== {action} Position opened: {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        entryOrder = self.openPositions.pop(position.getInstrument())
        logger.info(
            f"{execInfo.getDateTime()} ===== Exited {position.getEntryOrder().getInstrument()} at <{execInfo.getPrice()}> with quantity<{execInfo.getQuantity()}> =====")

    def onEnterCanceled(self, position):
        logger.info(
            f"===== Entry Position cancelled: {position.getInstrument()} =====")

    def onExitCanceled(self, position):
        logger.info(
            f"===== Exit Position canceled: {position.getInstrument()} =====")

    def onResampledBars(self, bars):
        if len(self.getActivePositions()):
            self.state = State.PLACING_ORDERS
            logger.info('Exiting positions')
            for position in self.getActivePositions().copy():
                if position.getEntryOrder().isFilled():
                    position.exitMarket()

    def haveLTP(self, instrument):
        return instrument in self.getFeed().getKeys() and len(self.getFeed().getDataSeries(instrument)) > 0

    def onBars(self, bars):
        logger.info(bars.getDateTime())
        if self.Position != None and bars.getDateTime() > datetime.timedelta(seconds=10):
            self.state = State.PLACING_ORDERS
            self.Position.exitMarket()
            self.orderTime = None

        if self.state == State.LIVE and len(self.getActivePositions()) == 0:
            self.state = State.PLACING_ORDERS
            logger.info('Initiating trade')
            for i in x:
                if self.haveLTP(i) != False:
                    price = self.getFeed().getDataSeries(i)[-1].getClose()
                    self.Position = strategy.enterLong(i, 15)
                    self.orderTime = bars.getDateTime()

        elif self.state == State.PLACING_ORDERS:
            if len(self.getActivePositions()):
                self.state = State.LIVE


def main():
    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)
        cred = creds['kotak']

    api = NeoAPI(consumer_key=cred['consumer_key'],
                 consumer_secret=cred['consumer_secret'], environment=cred['environment'])
    api.login(mobilenumber=cred['mobilenumber'], password=cred['Password'])
    ret = api.session_2fa(cred['mpin'])

    if ret['data'] != None:
        # feed = CustomCSVFeed.CustomCSVFeed()
        # feed.addBarsFromParquets(
        #    dataFiles=["pyalgomate/backtesting/data/test.parquet"], ticker='BANKNIFTY')
        # tokenMappings = getTokenMappings(
        #     api, ['BANKNIFTY2381744100CE'])
        tokenMappings = [
            {'instrument_token': '50812', 'exchange_segment': 'nse_fo',
                'instrument': 'BANKNIFTY2382443800CE'}]

        # Remove NFO| and replace index names
        # for key, value in tokenMappings.items():
        #     tokenMappings[key] = value.replace('NFO|', '').replace('NSE|NIFTY BANK', 'BANKNIFTY').replace(
        #         'NSE|NIFTY INDEX', 'NIFTY')

        feed = LiveTradeFeed(api, tokenMappings)
        broker = LiveBroker(api)
        # broker = PaperTradingBroker(100000, feed)
        intradayData = IntradayData(feed, broker)
        print(tokenMappings)

        return intradayData


if __name__ == "__main__":
    strategy = main()
    strategy.run()
