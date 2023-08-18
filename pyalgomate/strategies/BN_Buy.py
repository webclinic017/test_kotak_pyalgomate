from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State
from pyalgomate.cli import CliMain
import logging
import time
import pyalgotrade.bar

'''
Deploy 3 PM buying strategy
'''


class BN_Buy(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, registeredOptionsCount=0, callback=None, resampleFrequency=None, lotSize=15, collectData=None):
        super(BN_Buy, self).__init__(feed, broker,
                                     strategyName=__class__.__name__,
                                     logger=logging.getLogger(
                                         __file__),
                                     callback=callback,
                                     collectData=collectData)

        self.entryTime = datetime.time(hour=15, minute=00)
        self.exitTime = datetime.time(hour=15, minute=25)
        self.lotSize = 15
        self.lots = 10
        self.quantity = self.lotSize * self.lots
        self.underlying = underlying
        self.strikeDifference = 100
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback
        # Buy init codes
        self.threshold_percent = 9
        self.portfolioProfit = 6000
        self.portfolioSL = 10000
        self.initialLtpCE = None
        self.initialLtpPE = None

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positions = []

    def closeAllPositions(self):
        self.state = State.PLACING_ORDERS
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
        self.state = State.EXITED

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        currentWeekExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        elif (bars.getDateTime().time() >= self.exitTime):
            if (self.state != State.EXITED) and (len(self.openPositions) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                self.closeAllPositions()
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            ltp = self.getLTP(self.underlying)

            if ltp is None:
                return

            atmStrike = self.getATMStrike(ltp, self.strikeDifference)
            atmCEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, atmStrike, 'c')
            atmPEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, atmStrike, 'p')

            atmCELTP = self.getLTP(atmCEWeekly)
            atmPELTP = self.getLTP(atmPEWeekly)

            if atmCELTP is None or atmPELTP is None:
                return

            atmCELTP = self.getLTP(atmCEWeekly)
            atmPELTP = self.getLTP(atmPEWeekly)

            if self.initialLtpCE is None:
                self.initialLtpCE = atmCELTP
            if self.initialLtpPE is None:
                self.initialLtpPE = atmPELTP

            if self.initialLtpCE is not None and self.initialLtpCE != 0:
                percent_increase_ce = (
                    (atmCELTP - self.initialLtpCE) / self.initialLtpCE) * 100
            else:
                percent_increase_ce = 0

            if self.initialLtpPE is not None and self.initialLtpPE != 0:
                percent_increase_pe = (
                    (atmPELTP - self.initialLtpPE) / self.initialLtpPE) * 100
            else:
                percent_increase_pe = 0

            # self.log(f"CE LTP increase: {percent_increase_ce:.2f}%")
            # self.log(f"PE LTP increase: {percent_increase_pe:.2f}%")

            if percent_increase_ce >= self.threshold_percent:
                self.state = State.PLACING_ORDERS
                self.positions.append(
                    self.enterLong(atmCEWeekly, self.quantity))

            elif percent_increase_pe >= self.threshold_percent:
                self.state = State.PLACING_ORDERS
                self.positions.append(
                    self.enterLong(atmPEWeekly, self.quantity))

            # self.state = State.PLACING_ORDERS
            # self.positions.append(self.enterLong(atmCEWeekly, self.quantity))
            # self.positions.append(self.enterLong(atmPEWeekly, self.quantity))

        elif self.state == State.ENTERED:
            if self.overallPnL >= self.portfolioProfit:
                self.log(
                    f'Profit <{self.overallPnL}> has reached portfolio take profit <{self.portfolioProfit}>. Exiting all positions')
                self.closeAllPositions()
            elif self.overallPnL <= -self.portfolioSL:
                self.log(
                    f'Loss <{self.overallPnL}> has reached portfolio stop loss <{self.portfolioSL}>. Exiting all positions')
                self.closeAllPositions()


if __name__ == "__main__":
    CliMain(BN_Buy)
