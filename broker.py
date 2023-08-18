import enum
import json
import math
from greeks import fetch_greeks
import threading
import numpy as np
import pandas as pd
from neo_api_client import NeoAPI
import account_config as ac
from chart_data import get_time_price_series
from scripmaster import get_fo_scriptmaster
from pymongo import MongoClient
from new_parameter import *
import datetime as dt
from pytz import timezone
from telegram import report
import new_parameter
import uuid


IST = timezone('Asia/Kolkata')

def generate_six_digit_id():
    # Generate a random UUID
    random_uuid = uuid.uuid4()
    
    # Convert the UUID to a hexadecimal string
    hex_string = random_uuid.hex
    
    # Extract the first 6 characters from the hexadecimal string
    six_digit_id = hex_string[:10].upper()
    
    return six_digit_id

class OrderType(enum.Enum):
    LIMIT = 'L'
    MARKET = 'MKT'
    STOP_LOSS_LIMIT = 'SL'

class Exchange(enum.Enum):
    NSE_CM = 'nse_cm'
    
    BSE_CM = 'bse_cm'
   
    NSE_FO = 'nse_fo'
   
    BSE_FO = 'bse_fo'
   
    CDE_FO = 'cde_fo'
    
    BCS_FO = 'bcs_fo'

class ProductCode(enum.Enum):
    NRML = 'NRML'  # Normal
    CNC = 'CNC'  # Cash and Carry
    MIS = 'MIS'  # MIS
    INTRADAY = 'INTRADAY'  # Intraday
    CO = 'CO'  # Cover Order
    BO = 'BO'  # Bracket Order

class TransactionType(enum.Enum):
    BUY = "B"
    SELL = "S"

live_feed = {}

fno_script_master = get_fo_scriptmaster()
#fno_script_master = pd.read_csv("fno_scrip.csv")
fno_script_master["expiry_date"] = pd.to_datetime(fno_script_master['expiry_date'])

def on_message(message):
    global live_feed
    for data in message:
        try:
            live_feed.update({data["tk"]:float(data["ltp"])})
        except:
            pass

def on_error(message):
    result = message
    print('[OnError]: ', result)
    
def on_open(message):
    print('[OnOpen]: ', message)
    
def on_close(message):
    print('[OnClose]: ', message)

def get_trade_config():
    with open('parameter.json') as json_file:
        json_data = json.load(json_file)
    today_date = dt.datetime.now(tz=IST).date()
    today_date = pd.Timestamp(today_date,tz=IST)
    today_day = today_date.day_name().lower()

    # expiry_date = pd.Timestamp(expiry_date,tz=IST)

    # if expiry_date == today_date:
    #     if index_name == "Nifty Fin Service":
    #         today_day = "tuesday"
    #     else:
    #         today_day = "thursday"
    
    return json_data[today_day]

# Function to perform MongoDB operations
def mongo_operations():
    CONNECTION_STRING = new_parameter.connection_string
    mongo_client = MongoClient(CONNECTION_STRING)
    databases = mongo_client.list_database_names()
    account_json = {"_id": ac.user_id}

    # Create "trading_data" database if it doesn't exist
    if "trading_data" not in databases:
        mydb = mongo_client["trading_data"]

    collist = mongo_client["trading_data"].list_collection_names()

    # Create "trading_data" collection if it doesn't exist and insert account JSON
    if "trading_data" not in collist:
        mycol = mongo_client["trading_data"]["trading_data"]
        mycol.insert_one(account_json)
    else:
        doc_count = mongo_client["trading_data"]["trading_data"].count_documents(account_json)
        if doc_count == 0:
            mycol = mongo_client["trading_data"]["trading_data"]
            mycol.insert_one(account_json)

    # Create "login_creds" collection if it doesn't exist and insert account JSON
    if "login_creds" not in collist:
        mycol = mongo_client["trading_data"]["login_creds"]
        mycol.insert_one(account_json)
    else:
        doc_count = mongo_client["trading_data"]["login_creds"].count_documents(account_json)
        if doc_count == 0:
            mycol = mongo_client["trading_data"]["login_creds"]
            mycol.insert_one(account_json)

    return mongo_client["trading_data"]["trading_data"], mongo_client["trading_data"]["login_creds"]
# Function to login to the Kotak Neo
def login_kotak(mongo_client_creds):
    try:
        # Retrieve the session token from MongoDB
        session_token = mongo_client_creds.find_one({"_id": ac.user_id})['session_token']
        client = NeoAPI(consumer_key=ac.consumer_key, consumer_secret=ac.consumer_secret, environment='prod', on_message=on_message, on_error=on_error, on_open=on_open, on_close=on_close,access_token=session_token)
        client.login(pan=ac.pan, password=ac.user_pwd)
        client.session_2fa(OTP=ac.mpin)
        #report('Old Session Token Valid')
        return client
    except:
        # Login with credentials and generate a new session token
        client = NeoAPI(consumer_key=ac.consumer_key, consumer_secret=ac.consumer_secret, environment='prod', on_message=on_message, on_error=on_error, on_open=on_open, on_close=on_close)
        session_token = client.configuration.bearer_token
        client.login(pan=ac.pan, password=ac.user_pwd)
        client.session_2fa(OTP=ac.mpin)
        mongo_client_creds.update_one({"_id": ac.user_id}, {"$set": {"session_token": session_token}})
        report('Generating New Session Token')
        return client

# Function to calculate the ATM strike for Bank Nifty
def calculate_banknifty_atm(val):
    val2 = math.fmod(val, 100)
    val3 = 0
    if val2 >= 50:
        val3 = 100
    else:
        val3 = 0
    atm = val - val2 + val3
    return int(atm)

# Function to calculate the ATM strike for Nifty
def calculate_nifty_atm(val):
    val2 = math.fmod(val, 50)
    val3 = 0
    if val2 >= 25:
        val3 = 50
    else:
        val3 = 0
    atm = val - val2 + val3
    return int(atm)

# Function to calculate the ATM strike for Nifty
def calculate_midcap_atm(val):
    val2 = math.fmod(val, 25)
    val3 = 0
    if val2 >= 12.5:
        val3 = 25
    else:
        val3 = 0
    atm = val - val2 + val3
    return int(atm)

# Function to calculate the ATM strike based on the index symbol
def calculate_atm(index_symbol, index_ltp):
    if index_symbol == "NIFTY" or index_symbol == "FINNIFTY":
        atm_strike = calculate_nifty_atm(float(index_ltp))
    elif index_symbol == "BANKNIFTY":
        atm_strike = calculate_banknifty_atm(float(index_ltp))
    elif index_symbol == "MIDCPNIFTY":
        atm_strike = calculate_midcap_atm(float(index_ltp))
    
    return atm_strike

# Function to get a datetime object for a given hour and minute in IST timezone
def get_time_object(hour, minute):
    entry_time = dt.datetime(year=dt.datetime.now(tz=IST).year, month=dt.datetime.now(tz=IST).month, day=dt.datetime.now(tz=IST).day)
    entry_time = entry_time.replace(hour=hour, minute=minute)
    entry_time = pd.Timestamp(entry_time,tz=IST)
    return entry_time

# Function to get the current expiry date for the given index symbol
def get_current_expiry_date(index_symbol):
    df = fno_script_master
    #df["Expiry"] = pd.to_datetime(df["Expiry"])
    current_date = dt.datetime.now(tz=IST).date()
    scrip_master = (df.loc[(df["index_symbol"] == index_symbol) & (df["expiry_date"].dt.date >= current_date)]).sort_values(by=["expiry_date"])
    return (scrip_master['expiry_date'].iloc[0])

# Function to get the lot size for the given index symbol
def get_lot_size(index_symbol):
    df = fno_script_master
    scrip_master = (df.loc[df["index_symbol"] == index_symbol]).sort_values(by=["expiry_date"])
    return int(scrip_master['lot_size'].iloc[0])

def get_fno_instrument_token(index_symbol,expiry,strike,optionType):
    
    script = fno_script_master['instrument_token'].loc[(fno_script_master['index_symbol']==index_symbol) & (fno_script_master['expiry_date']==expiry) & (fno_script_master['strike'] == float(strike)) & (fno_script_master['option_type']==optionType)]
    return int(script.iloc[0])

def get_fno_instrument_symbol(index_symbol,expiry,strike,optionType):
    
    script = fno_script_master['option_symbol'].loc[(fno_script_master['index_symbol']==index_symbol) & (fno_script_master['expiry_date']==expiry) & (fno_script_master['strike'] == float(strike)) & (fno_script_master['option_type']==optionType)]
    return script.iloc[0]

def get_symbol_from_token(token):

    script = fno_script_master['option_symbol'].loc[(fno_script_master['instrument_token']==str(token))]
    return script.iloc[0]

def get_strike_from_token(token):

    script = fno_script_master['strike'].loc[(fno_script_master['instrument_token']==str(token))]
    return script.iloc[0]

def get_sl_value(avg_price,sl_type,sl_type_value):
    if sl_type == 'percentage':
        sl = avg_price * (1 + sl_type_value)
    elif sl_type == "points":
        sl = avg_price + sl_type_value

    return round(sl / 0.05) * 0.05

def get_target_value(avg_price,target_type,target_type_value):
    if target_type == "points":
        target_value = float(avg_price - target_type_value)
    elif target_type == "percentage":
        target_value = avg_price * (1 - target_type_value)
    if target_value <= 0.1:
        target_value = 0.15
    return round(target_value / 0.05) * 0.05

class OptionType(enum.Enum):
    Call = 'CE'
    Put = 'PE'

# Class to represent an index
class Index:
    def __init__(self):
        self.exchange = None
        self.expiry_date = None
        self.index_symbol = None
        self.spot_token = None
        self.lot_size = None
        self.strike_increment = None
        self.call_token_list=[]
        self.put_token_list=[]
        self.option_chain = []
        self.co_code = None  # used in kotak securitres


    def set_values(self, symbol):
        if symbol == "Nifty Bank":
            self.expiry_date = get_current_expiry_date('BANKNIFTY')
            #report("{}".format(self.expiry_date))
            self.exchange = "NSE"
            self.index_symbol = "BANKNIFTY"
            self.spot_token = "26009"
            self.lot_size = get_lot_size(self.index_symbol)
            self.strike_increment = 100
            self.co_code = 26753

        elif symbol == "Nifty 50":
            self.expiry_date = get_current_expiry_date('NIFTY')
            self.exchange = "NSE"
            self.index_symbol = "NIFTY"
            self.spot_token = "26000"
            self.lot_size = get_lot_size(self.index_symbol)
            self.strike_increment = 50
            self.co_code = 20559

        elif symbol == "Nifty Fin Service":
            self.expiry_date = get_current_expiry_date('FINNIFTY')
            self.exchange = "NSE"
            self.index_symbol = "FINNIFTY"
            self.lot_size = get_lot_size(self.index_symbol)
            self.spot_token = "26037"
            self.strike_increment = 50
            self.co_code = 41384

        elif symbol == "Nifty Midcap Select":
            self.expiry_date = get_current_expiry_date('MIDCPNIFTY')
            self.exchange = "NSE"
            self.index_symbol = "MIDCPNIFTY"
            self.lot_size = get_lot_size(self.index_symbol)
            self.spot_token = "26074"
            self.strike_increment = 25
            self.co_code = 75971
        



    def get_exchange(self):
        return self.exchange

    def get_expiry_date(self):
        return self.expiry_date

    def get_index_symbol(self):
        return self.index_symbol

    def get_spot_token(self):
        return self.spot_token

    def get_lot_size(self):
        return self.lot_size

    def get_strike_increment(self):
        return self.strike_increment

class Broker_Kotak:
    mongo_client, mongo_client_creds = mongo_operations()
    client = None

    def __init__(self):
        if not Broker_Kotak.client:
            Broker_Kotak.client = login_kotak(Broker_Kotak.mongo_client_creds)
        #self.shared_queue = Queue()
        self.order_status_complete_tag = "complete"
        if new_parameter.product_type == "NRML":
            self.product_type = ProductCode.NRML.value
        elif new_parameter.product_type == "MIS":
            self.product_type = ProductCode.MIS.value
    
    def get_instrument_current_candle_close(self, co_code=None,start_date=None,end_date=None,interval=1,index_symbol=None,expird_date=None,strike=None,option_type=None):
        df = get_time_price_series(co_code,dt.datetime.now(tz=IST)-dt.timedelta(days=2),dt.datetime.now(tz=IST),interval,index_symbol,expird_date,strike,option_type)
        #current_close = float(df["close"].iloc[-2])
        return float(df["close"].iloc[-2]),df["datetime"].iloc[-2]
    
    def get_time_series(self, co_code=None,start_date=None,end_date=None,interval=1,index_symbol=None,expird_date=None,strike=None,option_type=None):
        df = get_time_price_series(co_code,start_date,dt.datetime.now(tz=IST),interval,index_symbol,expird_date,strike,option_type)
        return df
    
    def get_instrument_morning_candle_close(self, co_code=None,start_date=None,end_date=None,interval=1,index_symbol=None,expird_date=None,strike=None,option_type=None):
        df = get_time_price_series(co_code,dt.datetime.now(tz=IST),dt.datetime.now(tz=IST),interval,index_symbol,expird_date,strike,option_type)
        current_close = float(df["close"].iloc[0])
        return current_close

    
    def get_trigger_value(self, orderbook, orderId):
        
        df = pd.DataFrame(orderbook)
        df = df.loc[df["nOrdNo"] == str(orderId)]
        return float(df["trgPrc"].iloc[-1])

    def run_broker_websocket(self,tokens=None):
        try:
        # Get live feed data
            self.client.subscribe(instrument_tokens=tokens, isIndex=False)
        except Exception as e:
            if tokens is None:
                pass
            else:
                print("Exception while connection to socket->socket: %s\n" % e)

    def subscribe_token_feed(self, exchange, token_list):
        if exchange == "NSE":
            exchange = "nse_cm"
        if exchange == "NFO":
            exchange = "nse_fo"

        if type(token_list) == list:
            inst_tokens = []
            for token in token_list:
                inst_tokens.append({"instrument_token": str(token), "exchange_segment": exchange})
        else:
            inst_tokens = []
            inst_tokens.append({"instrument_token": str(token_list), "exchange_segment": exchange})
        feed_thread = threading.Thread(target=self.run_broker_websocket, args=(inst_tokens,))
        feed_thread.start()

    def get_orderbook(self):
        return self.client.order_report()["data"]
    
    def get_pending_orders(self, orderbook):
        try:
            orders = pd.DataFrame(orderbook)
            orders = orders.loc[(orders["ordSt"] == "open") | (orders["ordSt"] == "trigger pending")]
            return orders
        except:
            return pd.DataFrame()
    
    def get_orderid_status(self,orderbook):
        try:
            orders = pd.DataFrame(orderbook)
            orders_status = orders[["nOrdNo", "ordSt"]].copy()
            orders_status.loc[:, "nOrdNo"] = orders_status.loc[:, "nOrdNo"].astype(np.int64)
            orders_status = dict(zip(orders_status["nOrdNo"], orders_status["ordSt"]))
            return orders_status
        except Exception as e:
            return {}
    
    def get_all_order_id(self, orderbook):
        try:
            orders = pd.DataFrame(orderbook)
            order_id_list = [int(i) for i in orders["nOrdNo"].to_list()]
            return order_id_list
        except:
            return []
        
    def buy_market_order(self,quantity,token,tag,is_amo=False):
    
        symbol = get_symbol_from_token(token)

        exchange = Exchange.NSE_FO.value
        pc = self.product_type
        order_type = OrderType.MARKET.value
        transaction_type = TransactionType.BUY.value

        tag = generate_six_digit_id() + f"_{tag}"

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        report( str(tag) +" : " +"Buy Order = {}".format(symbol))

        order = self.client.place_order(exchange, pc, "0", order_type, str(quantity), "DAY", symbol,
                    transaction_type, amo=amo, disclosed_quantity="0", market_protection="0", pf="N",
                    trigger_price="0", tag=tag)
        orderID = order["nOrdNo"]

        return orderID
    
    def sell_market_order(self,quantity,token,tag,is_amo=False):
    
        symbol = get_symbol_from_token(token)

        exchange = Exchange.NSE_FO.value
        pc = self.product_type
        order_type = OrderType.MARKET.value
        transaction_type = TransactionType.SELL.value

        tag = generate_six_digit_id() + f"_{tag}"

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        report( str(tag) +" : " +"Sell Order = {}".format(symbol))

        order = self.client.place_order(exchange, pc, "0", order_type, str(quantity), "DAY", symbol,
                    transaction_type, amo=amo, disclosed_quantity="0", market_protection="0", pf="N",
                    trigger_price="0", tag=tag)
        orderID = order["nOrdNo"]

        return orderID
    
    def place_stoploss_order(self,tag,quantity,token,avg_price,strategy_parameter,is_amo=False):

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        symbol = get_symbol_from_token(token)

        tag = generate_six_digit_id() + f"_{tag}"

        exchange = Exchange.NSE_FO.value
        pc = self.product_type
        order_type = OrderType.STOP_LOSS_LIMIT.value
        transaction_type = TransactionType.BUY.value
    
        report( str(tag) +" : " +"Placing SL Order = {}".format(symbol))

        sl_price = get_sl_value(avg_price,strategy_parameter["sl"]["type"],strategy_parameter["sl"]["value"])

        #sl_price = avg_price
    
        order = self.client.place_order(exchange, pc, f"{sl_price + stop_loss_buffer}", order_type, str(quantity), "DAY", symbol,
                    transaction_type, amo=amo, disclosed_quantity="0", market_protection="0", pf="N",
                    trigger_price= f"{sl_price}", tag=tag)
        
        return order["nOrdNo"] , sl_price
    
    def place_target_order(self,tag,quantity,token,avg_price,strategy_parameter,is_amo=False):

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        symbol = get_symbol_from_token(token)

        tag = generate_six_digit_id() + f"_{tag}"

        exchange = Exchange.NSE_FO.value
        pc = self.product_type
        order_type = OrderType.LIMIT.value
        transaction_type = TransactionType.BUY.value
    
        report( str(tag) +" : " +"Placing SL Order = {}".format(symbol))

        target_value = get_target_value(avg_price,strategy_parameter["target_type"],strategy_parameter["target_points"])

        #target_value = avg_price
    
        order = self.client.place_order(exchange, pc, f"{target_value}", order_type, str(quantity), "DAY", symbol,
                    transaction_type, amo=amo, disclosed_quantity="0", market_protection="0", pf="N",
                    trigger_price= "0", tag=tag)
        
        return order["nOrdNo"] 
    
    def get_average_price(self,order_id, orderbook):

        orders = pd.DataFrame(orderbook)

        if str(order_id) not in orders["nOrdNo"].values:
            return -2

        order_status = orders.loc[orders["nOrdNo"] == str(order_id), "ordSt"].iloc[0]

        if order_status == "rejected":
            return -1

        avg_price = orders.loc[orders["nOrdNo"] == str(order_id), "avgPrc"].iloc[0]
        return float(avg_price)

    def modify_sl_order(self,order_id,quantity,avg_price,is_amo=False):

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        pc = self.product_type
        order_type = OrderType.STOP_LOSS_LIMIT.value
        transaction_type = TransactionType.BUY.value

        self.client.modify_order( product = pc, price = f"{avg_price + stop_loss_buffer}", 
                        order_type = order_type, quantity= f"{quantity}",transaction_type = transaction_type, validity= "DAY" ,order_id = f"{order_id}",trigger_price= f"{avg_price}", amo = amo)
    
    def modify_to_market_order(self,order_id,quantity,is_amo=False):

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        pc = self.product_type
        order_type = OrderType.MARKET.value
        transaction_type = TransactionType.BUY.value

        self.client.modify_order( product = pc, price = "0", 
                        order_type = order_type, quantity= f"{quantity}",transaction_type = transaction_type,  validity= "DAY",order_id = f"{order_id}",trigger_price= "0", amo = amo)

    def cancel_order(self,orderId,is_amo = False):

        if is_amo:
            amo = "YES"
        else:
            amo = "NO"

        self.client.cancel_order(str(orderId),amo)


    def get_fno_tokens_list(self,index_obj):  
        tokens=[]
        x=0

        index_ltp = self.get_index_ltp(index_obj.spot_token)
        
        index_atm = calculate_atm(index_obj.index_symbol,index_ltp)

        def fetch_instrument_and_append(strike):
            try:
                call_token = get_fno_instrument_token(index_obj.index_symbol,index_obj.expiry_date,strike,"CE")

                index_obj.call_token_list.append(call_token)
            except:
                call_token = None
            try:
                put_token = get_fno_instrument_token(index_obj.index_symbol,index_obj.expiry_date,strike,"PE")
               
                index_obj.put_token_list.append(put_token)
            except:
                put_token = None

            if call_token is not None and put_token is not None:
                index_obj.option_chain.append({"strike":strike,"call_token":call_token,"put_token":put_token,"expiry_date":index_obj.expiry_date})

            if call_token is None and put_token is not None:
                index_obj.option_chain.append({"strike":strike,"put_token":put_token,"expiry_date":index_obj.expiry_date})
            
            if call_token is not None and put_token is None:
                index_obj.option_chain.append({"strike":strike,"call_token":put_token,"expiry_date":index_obj.expiry_date})

        for i in range(50): 
            fetch_instrument_and_append(index_atm + x)
            if i>0:
                fetch_instrument_and_append(index_atm - x)
            x += index_obj.strike_increment

    def get_ltp(self,token):
        if str(token) in live_feed:
            return live_feed[str(token)]
        else:
            return None
    
    def get_index_ltp(self,token):
        if str(token) in live_feed:
            return live_feed[str(token)]
        else:
            return self.client.quotes(instrument_tokens=[{"instrument_token": f"{token}", "exchange_segment": "nse_cm"}], quote_type="", isIndex=False)["message"][0]["last_traded_price"]
        
    def transform_option_chain(self,df,index_obj):

        df['call_ltp'] = df['call_token'].map(self.get_ltp)
        df['put_ltp'] = df['put_token'].map(self.get_ltp)

        S = self.get_index_ltp(index_obj.spot_token) # Current stock price
        r = 0.1  # Risk-free interest rate


        greeks = []
        for _, row in df.iterrows():
            try:
                #greeks_values = caculate_greeks(datetime.now(tz=pytz.timezone("Asia/Kolkata")), row['expiry_date'], S, float(row['strike']), r, float(row['call_ltp']), float(row['put_ltp']))
                greeks_values = fetch_greeks(S,float(row['strike']),dt.datetime.now(tz=IST),row['expiry_date'],float(row['call_ltp']), float(row['put_ltp']),r)
                greeks.append(greeks_values)
            except Exception as e:
                print(e)
                pass
        greeks_df = pd.DataFrame(greeks)
        df = pd.concat([df, greeks_df], axis=1)
        df["put_delta"] = df["put_delta"] * -1
        df = df[['call_token',"call_theta","call_delta","call_vega","call_ltp","strike","put_ltp","put_vega","put_delta","put_theta",'put_token']]
        return df.sort_values('strike')

    def upper_lower_range(self,index_obj):

        gapup_gapdown_condn = True

        opening_close = self.get_instrument_morning_candle_close(index_obj.co_code,interval="5")

        df = self.get_time_series(index_obj.co_code,dt.datetime.now(tz=IST) - dt.timedelta(days=7),interval="D")

        upper_range_1 , lower_range_1 = df["high"].iloc[-3],df["low"].iloc[-3]

        upper_range_2 , lower_range_2 = df["high"].iloc[-2],df["low"].iloc[-2]

        upper_range_pct = (abs(max(upper_range_1,upper_range_2) - opening_close)/opening_close)*100
        lower_range_pct = (abs(min(lower_range_1,lower_range_2) - opening_close)/opening_close)*100

        if upper_range_pct >=1 :
            upper_range = min(upper_range_1,upper_range_2)
        else:
            upper_range = max(upper_range_1,upper_range_2)

        if lower_range_pct >=1 :
            lower_range = max(lower_range_1,lower_range_2)
        else:
            lower_range = min(lower_range_1,lower_range_2)

        if opening_close>lower_range and opening_close<upper_range:
            pct_value = min(upper_range_pct,lower_range_pct)
            upper_range = opening_close * (1+(pct_value/100))
            lower_range = opening_close * (1-(pct_value/100))
            gapup_gapdown_condn = False


        return {"upper_range":upper_range,"lower_range":lower_range,"middle_range":(upper_range+lower_range)/2,"gp_cond":gapup_gapdown_condn}