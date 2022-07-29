#Note: Restart the program after you exit a position yourself
from ibapi.wrapper import *
from ibapi.contract import *
from ibapi.client import *
from ibapi import wrapper
from ibapi.common import *

import time
import string
import numpy
from datetime import datetime, timedelta

import yfinance as yf #MODIFIED TO OMIT "COMPLETED" BAR
import pandas as pd
import pandas_datareader.data as web

from time import sleep, perf_counter
from threading import Thread

import threading

class tickerConnection(EWrapper, EClient):
    def start(self):
        sleep(3)
        
        self.reqMarketDataType(3)
        
        global SP_INFO        
        global SP_CONTRACTS 
        
        SP_CONTRACTS = {}
        
        for i in SP_INFO: #Create contracts for all stocks traded this session
            
            SP_CONTRACTS[i] = Contract()
            SP_CONTRACTS[i].secType = 'STK'
            SP_CONTRACTS[i].currency = 'USD' 
            
            #Mofify this when the list of stocks, or symbols changes
            if SP_INFO[i][0] == "CSCO" or SP_INFO[i][0] == "META": 
                SP_CONTRACTS[i].primaryExchange = "ISLAND" 
            elif SP_INFO[i][0] == "CAT":
                SP_CONTRACTS[i].primaryExchange = "NYSE"            
            #else:
                #SP_CONTRACTS[i].exchange = 'SMART'
            SP_CONTRACTS[i].exchange = 'SMART'    
            SP_CONTRACTS[i].symbol = SP_INFO[i][0]
            
            
            self.reqMktData(i, SP_CONTRACTS[i], "", False, False, []) 
            self.reqMarketDataType(3)#Delayed ticker prices            

            sleep(0.03) #50 requests allowed per second

    def tickPrice(self, reqId, tickType, price, attrib):
        global SP_INFO
        global SP_CONTRACTS         
        #Checks if market is in open hours
        hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60
        if(hours > 16):    
            self.disconnect()
           
        if tickType == 66 or tickType == 1:

            today = datetime.now()

            SP_INFO[reqId][3] = price
            SP_INFO[reqId][4] = str(today)
            
            if price == -1: 
                print(SP_INFO[reqId][0], "failed to get price")
                defined = False
            elif (SP_INFO[reqId][1] < 1) and (SP_INFO[reqId][1] > 0):
                defined = False
            elif(SP_INFO[reqId][8] == -1) or (SP_INFO[reqId][9] == -1):
                defined = False
            elif (SP_INFO[reqId][10] == -1) or (SP_INFO[reqId][11] == -1):
                defined = False
            elif (SP_INFO[reqId][12] == -1) or (SP_INFO[reqId][13] == -1):
                defined = False            
            else:
                defined = True
                #print("undefined", SP_INFO[reqId])
                
            if (hours >= 9.5 ) and (defined == True):
                global SP_CONTRACTS
                global CASH
                #print("tik", SP_INFO[reqId])
                if(SP_INFO[reqId][1] == 0): #Entry
                
                    if (price <= SP_INFO[reqId][7]) and (SP_INFO[reqId][9] > SP_INFO[reqId][13] + (SP_INFO[reqId][5]/6) ): #Short Entry
                        print("TRADE: ENTRY, SHORT", SP_INFO[reqId][0])
                        print(SP_INFO[reqId])
                        print(CASH)
                        
                        order = Order()
                        order.action = 'SELL'
                        
                        order.totalQuantity = max(1, (CASH[1]* 0.02)//(SP_INFO[reqId][3]) ) #Find new position
                        
                        open_positions = 0
                        
                        for i in SP_INFO:
                            if (SP_INFO[i][1] != 0):
                                open_positions =  open_positions + 1 
                        print("Open positions:", open_positions)
                        
                        if ((price + 0.01) * order.totalQuantity < CASH[0]) and (open_positions < 51):
                            order.orderType = 'MKT'                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 63/64 #Position is undefined untill position checks it                               
                            
                        else:
                            print("Not enough funds, not exectuted.")
                        
                        print("Price", price, "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   
                            
                    elif (price >= SP_INFO[reqId][6]) and (SP_INFO[reqId][11] < SP_INFO[reqId][12] - (SP_INFO[reqId][5]/6) ): #Long Entry
                        print("TRADE: ENTRY, LONG", SP_INFO[reqId][0])
                        print(SP_INFO[reqId])
                        print(CASH)
                        
                        order = Order()
                        order.action = 'BUY'
                        
                        order.totalQuantity = max(1, (CASH[1]*0.02)//(SP_INFO[reqId][3]) ) #Find new position
                        
                        open_positions = 0
                        
                        for i in SP_INFO:
                            if (SP_INFO[i][1] != 0):
                                open_positions =  open_positions + 1   
                        print("Open positions:", open_positions)
                        
                        if ((price + 0.01) * order.totalQuantity < CASH[0]) and (open_positions < 51):
                            order.orderType = 'MKT'                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 63/64 #Position is undefined untill position checks it                               
                            
                        else:
                            print("Not enough funds, not exectuted.")
                        
                        print("Price", price, "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   

                elif(SP_INFO[reqId][1] >= 1): #Long Exit strategy
                    
                    if (price < SP_INFO[reqId][2] - SP_INFO[reqId][5]):
                        
                        print("TRADE: UNPROFITABLE EXIT, LONG", SP_INFO[reqId][0])
                        print(SP_INFO[reqId])
                        print(CASH)
                        
                        order = Order()
                        order.action = 'SELL'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Bought at", SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 63/64 #Position is undefined
                    
                    elif price > (SP_INFO[reqId][2] * 1.01): 
                        if (SP_INFO[reqId][8] > SP_INFO[reqId][9]) or  SP_INFO[reqId][9] > SP_INFO[reqId][13] + (1.75 * SP_INFO[reqId][5]): #Cash out
                            
                            print("TRADE: PROFITABLE EXIT, LONG", SP_INFO[reqId][0])
                            print(SP_INFO[reqId])
                            print(CASH)
                            
                            order = Order()
                            order.action = 'SELL'
                            order.totalQuantity = SP_INFO[reqId][1]
                            order.orderType = 'MKT'
                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            
                            print("Price", price, "Bought at", SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   
                            
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 63/64 #Position is undefined
                            
                elif(SP_INFO[reqId][1] < 0): #Exit Short strategy
                    if (price > SP_INFO[reqId][2] + SP_INFO[reqId][5]): #Stop loss
                       
                        print("TRADE: UNPROFITABLE EXIT, SHORT", SP_INFO[reqId][0])
                        print(SP_INFO[reqId])
                        print(CASH)
                        
                        order = Order()
                        order.action = 'BUY'
                        order.totalQuantity = (-1) * SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Shorted at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 63/64 #Position is undefined
                   
                    elif price < (SP_INFO[reqId][2] * 0.99):
                        if (SP_INFO[reqId][10] < SP_INFO[reqId][11]) or (SP_INFO[reqId][11] < SP_INFO[reqId][12] - (1.75 * SP_INFO[reqId][5])): #Cash out
                            print("TRADE: PROFITABLE EXIT, SHORT", SP_INFO[reqId][0])
                            print(SP_INFO[reqId])
                            print(CASH)
                            
                            order = Order()
                            order.action = 'BUY'
                            order.totalQuantity = (-1) * SP_INFO[reqId][1]
                            order.orderType = 'MKT'
                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            
                            print("Price", price, "Shorted at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", today, "\n")   
                            
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 63/64 #Position is undefined
        
    def __init__(self):
        EClient.__init__(self, self)
        self.nextOrderID = 0

    def nextValidId(self, orderId):
        self.nextOrderID = orderId
        self.start()

class accountConnection(EWrapper, EClient):#Cash balance and positions
    def start(self):
        self.reqAccountSummary(21, "All", "$LEDGER:USD")

    def __init__(self):
        EClient.__init__(self, self)
        self.nextOrderID = 0

    def nextValidId(self, orderId):
        self.nextOrderID = orderId
        self.start()

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        seconds = time.time()
        local_time = time.ctime(seconds)
        global SP_INFO
        global SP_INDEX        
        
        if (contract.secType == "STK"):
            
            if (SP_INFO[SP_INDEX[contract.symbol]][1] >= 1) or (SP_INFO[SP_INDEX[contract.symbol]][1] <= 8/64):
                SP_INFO[SP_INDEX[contract.symbol]][1] = position
                SP_INFO[SP_INDEX[contract.symbol]][2] = avgCost
                
            if (SP_INFO[SP_INDEX[contract.symbol]][1] < 1) and (SP_INFO[SP_INDEX[contract.symbol]][1] > 0): 
                print("unknowing you", contract.symbol, SP_INFO[SP_INDEX[contract.symbol]][1], position)
        
        elif (contract.secType == "CASH"):
            
            for i in SP_INFO:
                if (0 < SP_INFO[i][1] < 1):
                    SP_INFO[i][1] = SP_INFO[i][1] - 1/64
                    print("position defining", SP_INFO[i][0], SP_INFO[i][1])
                    
            #Cash shows up once per cycle
            #Continues if market is in opening hours
            
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60            
            if (hours < 16): 
                time.sleep(15)
                self.reqAccountSummary(21, "All", "$LEDGER:ALL")
            elif hours > 16:
                self.disconnect()             

    def accountSummary(self, reqId, account, tag, value, currency):#FOUR
        global CASH
        #print(tag, value, currency)
        if (tag == "NetLiquidationByCurrency" and currency == "USD"):        
            CASH[1] = float(value)
            
        if (tag == "CashBalance" and currency == "USD"):                    
            CASH[0] = float(value)
            #Continues if market is in opening hours
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60
            if (hours < 16): 
                time.sleep(15)
                self.reqPositions()
                #print("helosgbdhd")
            elif hours > 16:
                self.disconnect()  

def tickerThread(): 
    global tickerSocket
    tickerSocket.run()
    
def accountThread():
    global accountSocket
    accountSocket.run()
    
def highsLowsThread():   
    global SP_INFO 
    global CASH
    hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60
    #Quit when market closes
    while(hours < 16):
        for i in SP_INFO:
            #Only start requesting data once market opens
            if hours >= 9.5:
                if(SP_INFO[i][0] == "BRK B"):
                    data = yf.download(tickers= "BRK-B", period="2d", interval="15m") 
                    
                else:
                    data = yf.download(tickers= SP_INFO[i][0], period="2d", interval="15m")                 
                if len(data.index) > 19 :
                    
                    vol = 0

                    max5h = data.at[data.index[len(data.index) - 20], 'High']
                    min5h = data.at[data.index[len(data.index) - 20], 'Low']
                    
                    max2h30m = data.at[data.index[len(data.index) - 10], 'High']
                    min2h30m = data.at[data.index[len(data.index) - 10], 'Low']
                     
                    max45m = data.at[data.index[len(data.index) - 3], 'High']
                    min45m = data.at[data.index[len(data.index) - 3], 'Low']                     
    
                    #Get 5 hour high and low
                    for x in range(len(data.index) - 20, len(data.index)):
                        
                        max5h = max(max5h, data.at[data.index[x], 'High'])
                        min5h = min(min5h, data.at[data.index[x], 'Low'])
                        vol = vol + data.at[data.index[x], 'Volume']
                        
                        #Get 2.5 hour high and low
                        if x > len(data.index) - 10:
                            max2h30m = max(max2h30m, data.at[data.index[x], 'High'])
                            min2h30m = min(min2h30m, data.at[data.index[x], 'Low'])

                            #Get 0.75 hour high and low
                            if x > len(data.index) - 3:
                                max45m = max(max2h30m, data.at[data.index[x], 'High'])
                                min45m = min(min2h30m, data.at[data.index[x], 'Low'])                                    

                    SP_INFO[i][8] = max5h
                    SP_INFO[i][9] = max2h30m
                    
                    SP_INFO[i][10] = min5h
                    SP_INFO[i][11] = min2h30m  
                    
                    SP_INFO[i][12] = max45m
                    SP_INFO[i][13] = min45m 
                    
                    
                    SP_INFO[i][14] = vol/20

                    SP_INFO[i][15] = data.at[data.index[len(data.index) - 2], 'Volume']
                    #print("succs  ",  SP_INFO[i], datetime.now())
                    
                else:
                    print("Failed hrs",  SP_INFO[i], len(data.index), datetime.now())
                    
                    SP_INFO[i][8] = -1
                    SP_INFO[i][9] = -1
                    
                    SP_INFO[i][10] = -1
                    SP_INFO[i][11] = -1     
                    
                    SP_INFO[i][12] = -1                    
                    #print(data)
                    
                sleep(5)    
        hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60

def main():   
    today = datetime.now()
    startDate = str(today - timedelta(days=30))[0:10]
    endDate = str(today - timedelta(days=1))[0:10]
    
    global SP_INFO
    SP_INFO ={} 
    
    global SP_INDEX
    SP_INDEX ={}  
    
    global CASH
    CASH = [-1, -1] # 1) Cash balance 2) Net value
    
    i = 0
    
    #Read list of stocks to be traded
    tsx_list = open("s&p100.txt", "r")    
    print("Retrieving 30 day highs and lows...")
    for x in tsx_list:  
        
        symbol = x.strip()        
        i = i + 1      

        try:
            #Get pandas dataframe of stock info over last days
            if(symbol == "BRK B"):
                data = web.DataReader("BRK-B", 'yahoo', startDate, endDate)                
            else:
                data = web.DataReader(symbol, 'yahoo', startDate, endDate)

            #Get min, max over last 30 days
            thirtydaymin = 100000000
            thirtydaymax = 0
            atr = 0
            
            for j in range(0, len(data.index)):
                thirtydaymin = min(data.at[data.index[j], 'Low'], thirtydaymin)
                thirtydaymax = max(data.at[data.index[j], 'High'], thirtydaymax)
                
                if j in range(len(data.index) - 10, len(data.index)): #Get 10 day atr
                    a = data.at[data.index[j], 'High'] - data.at[data.index[j], 'Low']
                    b = abs(data.at[data.index[j], 'High'] - data.at[data.index[j - 1], 'Close'])
                    c = abs(data.at[data.index[j], 'Low'] - data.at[data.index[j - 1], 'Close'])
                    atr = atr + max(a, b, c)
                    previous = data.at[data.index[j - 1], 'Close']   
                    
            SP_INFO[i] = [symbol, 5/64, -1, -1, "NULL", atr/20, thirtydaymax, thirtydaymin, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1] 
            SP_INDEX[symbol] = i            
   
            #0) SYMBOL
            #1) POSITION, 2) PRICE BOUGHT AT
            #3) CURRENT PRICE, 4) LAST UPDATED, 
            #5) ATR, 6)thirty day MAX, 7) thirty day MIN  
            #8) 5 hour high, 9) 2.5 hour high,
            #10) 5 hour low, 11) 2.5 hour low,
            #12) 0.75 hour high, 13) 0.75 hour low,
            
            #14) Volume avg, 15) Volume
            
            # 1) Numbers between zero and one are undefined positions
            #Allowing positions to cycle several times before a stock's 
            #position is defined prevents duplicate trades.
            
            #Do not change the fractions to a number that cannot be stored
            #exactly as a float.
            #Allowing the position to be undefined for a time gives the application time to complete a trade
            #before it updates the position. The data stream does not notify when the position is 0.
            
        except Exception as e:
            print(e)
            print("Could not retrive", symbol)
    print("Retrieved")
    global tickerSocket
    global accountSocket
    
    tickerSocket = tickerConnection()
    accountSocket = accountConnection()  
    
    tickerSocket.connect('127.0.0.1', 7497, 0)
    accountSocket.connect('127.0.0.1', 7497, 1)
    
    sleep(1)
    
    TT = Thread(target=tickerThread, daemon=True) #Tickers and trading
    HLT = Thread(target=highsLowsThread, daemon=True)#Highs and Lows for last 5 hours  
    AT = Thread(target=accountThread, daemon=True) #Summary and positions 
    
    TT.start()
    HLT.start()    
    AT.start()
    
    TT.join()
    AT.join()
    HLT.join()
    
    tickerSocket.disconnect()
    accountSocket.disconnect()    
    print("\n")
    print("Ended Trading")
     
main()
