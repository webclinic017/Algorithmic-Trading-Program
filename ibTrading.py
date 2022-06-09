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
        
        for i in SP_INFO:
            
            self.reqMarketDataType(3)
            #Create contracts for all stocks traded this session
            SP_CONTRACTS[i] = Contract()
            SP_CONTRACTS[i].secType = 'STK'
            SP_CONTRACTS[i].currency = 'USD' 
            
            if SP_INFO[i][0] == "CSCO":
                SP_CONTRACTS[i].exchange = "ISLAND" 
            elif SP_INFO[i][0] == "CAT":
                SP_CONTRACTS[i].exchange = "NYSE" 
            else:
                SP_CONTRACTS[i].exchange = 'SMART'
                
            SP_CONTRACTS[i].symbol = SP_INFO[i][0]
            
            #Request to stream delayed ticker prices, 587(Pl Price Delayed)
             
            self.reqMktData(i, SP_CONTRACTS[i], '587', False, False, []) 
            
            if SP_INFO[i][0] == "CSCO" or SP_INFO[i][0] == "CAT":
                SP_CONTRACTS[i].exchange = 'SMART'

            sleep(0.03) #50 requests allowed per second

    def tickPrice(self, reqId, tickType, price, attrib):
        
        #Checks if market is in open hours
        hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60
        if(hours > 16):    
            self.disconnect()
            
        if tickType == 66:
            global SP_INFO
            global SP_CONTRACTS  
            
            today = datetime.now()
            
            if price != -1:
                SP_INFO[reqId][3] = price
                SP_INFO[reqId][4] = str(today) 

            if (hours >= 9.5 ) and SP_INFO[reqId][1] != 1/3:
                global SP_CONTRACTS
                global CASH
                
                if(SP_INFO[reqId][1] == 0): #Entry
                
                    if price <= SP_INFO[reqId][7]: #Short Entry
                        print("TRADE: ENTRY, SHORT", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'SELL'
                        
                        order.totalQuantity = max(1, (CASH[0]*0.02)//(SP_INFO[reqId][3]) ) #Find new position
                        
                        open_positions = 0
                        
                        for i in SP_INFO:
                            if (SP_INFO[i][1] != 0):
                                open_positions =  open_positions + 1                     
                        
                        if ((price + 0.01) * order.totalQuantity < CASH[1]) and (open_positions < 51):
                            order.orderType = 'MKT'                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 1/3 #Position is undefined untill position checks it                               
                            
                        else:
                            print("Not enough funds, not exectuted.")
                        
                        print("Price", price, "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        sleep(20)
                            
                    elif price >= SP_INFO[reqId][6]: #Long Entry
                        print("TRADE: ENTRY, LONG", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'BUY'
                        
                        order.totalQuantity = max(1, (CASH[0]*0.02)//(SP_INFO[reqId][3]) ) #Find new position
                        
                        open_positions = 0
                        
                        for i in SP_INFO:
                            if (SP_INFO[i][1] != 0):
                                open_positions =  open_positions + 1                     
                        
                        if ((price + 0.01) * order.totalQuantity < CASH[1]) and (open_positions < 51):
                            order.orderType = 'MKT'                            
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                            self.nextOrderID += 1
                            SP_INFO[reqId][1] = 1/3 #Position is undefined untill position checks it                               
                            
                        else:
                            print("Not enough funds, not exectuted.")
                        
                        print("Price", price, "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        sleep(20)                 

                elif(SP_INFO[reqId][1] > 0): #Long Exit strategy
                    if price < SP_INFO[reqId][2] - SP_INFO[reqId][5]: #Stop loss
                        
                        print("TRADE: UNPROFITABLE EXIT, LONG", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'SELL'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Bought at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 1/3 #Position is undefined
                        sleep(20)
                    
                    elif price > SP_INFO[reqId][2] and SP_INFO[reqId][8] > SP_INFO[reqId][9]: #Cash out
                        
                        print("TRADE: PROFITABLE EXIT, LONG", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'SELL'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Bought at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 1/3 #Position is undefined
                        sleep(20)
                        
                elif(SP_INFO[reqId][1] < 0): #Exit Short strategy
                    if price > SP_INFO[reqId][2] + SP_INFO[reqId][5]: #Stop loss
                       
                        print("TRADE: UNPROFITABLE EXIT, SHORT", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'BUY'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Shorted at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 1/3 #Position is undefined
                        sleep(20)
                   
                    elif price <= SP_INFO[reqId][2] and SP_INFO[reqId][8] > SP_INFO[reqId][9]: #Cash out
                        print("TRADE: PROFITABLE EXIT, SHORT", SP_INFO[reqId][0])
                        
                        order = Order()
                        order.action = 'BUY'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        print("Price", price, "Shorted at", price, SP_INFO[reqId][2], "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                        
                        self.nextOrderID += 1
                        SP_INFO[reqId][1] = 1/3 #Position is undefined
                        sleep(20)
  
    def __init__(self): #ONE
        EClient.__init__(self, self)
        self.nextOrderID = 0

    def nextValidId(self, orderId): #TWO
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
        global SP_INFO
        global SP_INDEX
        if (contract.secType == "STK"):
           
            SP_INFO[SP_INDEX[contract.symbol]][1] = position
            SP_INFO[SP_INDEX[contract.symbol]][2] = avgCost
            
        if (contract.secType == "CASH"): #US cash shows up once per cycle
            
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60
            
            if (hours < 16): #Continues if market has not closed yet
                for i in SP_INFO:
                    if SP_INFO[i][1] == 1/3:
                        SP_INFO[i][1] = 0

                time.sleep(15)
                
                self.reqAccountSummary(21, "All", "$LEDGER:ALL")
            elif hours >= 16:
                self.disconnect()             

    def accountSummary(self, reqId, account, tag, value, currency):#FOUR
        global CASH
        
        if (tag == "CashBalance" and currency == "USD"):        
            CASH[0] = float(value)
            
        if (tag == "NetLiquidationByCurrency" and currency == "USD"): 
            CASH[1] = float(value)
            
            #Continues if market is in opening hours
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[14:16])/60
            if (hours < 16): 
                time.sleep(15)
                self.reqPositions()
            elif hours >= 16:
                self.reqPositions()
                self.disconnect()               

def tickerThread(): 
    global tickerSocket
    sleep(180) #Allow data to download before trading begins
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

                    max5h = data.at[data.index[len(data.index) - 20], 'High']
                    min5h = data.at[data.index[len(data.index) - 20], 'Low']
                    
                    vol = 0 
                    
                    max2h30m = data.at[data.index[len(data.index) - 10], 'High']
                    min2h30m = data.at[data.index[len(data.index) - 10], 'Low']              
    
                    #Get 5 hour high and low
                    for x in range(len(data.index) - 20, len(data.index)):
                        
                        max5h = max(max5h, data.at[data.index[x], 'High'])
                        min5h = min(min5h, data.at[data.index[x], 'Low'])
                        vol = vol + data.at[data.index[x], 'Volume']
                        
                        #Get 2.5 hour high and low
                        if x > len(data.index) - 10:
                            max2h30m = max(max2h30m, data.at[data.index[x], 'High'])
                            min2h30m = min(min2h30m, data.at[data.index[x], 'Low'])                    
        
                    SP_INFO[i][8] = max5h
                    SP_INFO[i][9] = max2h30m
                    
                    SP_INFO[i][10] = min5h
                    SP_INFO[i][11] = min2h30m     
                    
                    SP_INFO[i][12] = vol/20

                    SP_INFO[i][13] = data.at[data.index[len(data.index) - 1], 'Volume']
                    print("succeeded, ",  SP_INFO[i], datetime.now())
                    
                else:
                    print("failed, ",  SP_INFO[i][0], datetime.now())
                    
                    SP_INFO[i][8] = -1
                    SP_INFO[i][9] = -1
                    
                    SP_INFO[i][10] = -1
                    SP_INFO[i][11] = -1     
                    
                    SP_INFO[i][12] = -1                    
                    print(data)
                
                sleep(5)
        print(CASH)
    
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
    CASH = [0, 0]
    # 1) Cash balance 2) Net value
    
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
            thirtydaymin = 1000000000
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
                    
            SP_INFO[i] = [symbol, 1/3, -1, -1, "NULL", atr/10, thirtydaymax, thirtydaymin, -1, -1, -1, -1, -1, -1] 
            SP_INDEX[symbol] = i            
            
            #0) SYMBOL
            #1) POSITION, 2) PRICE BOUGHT AT
            #3) CURRENT PRICE, 4) LAST UPDATED, 
            #5) ATR, 6)thirty day MAX, 7) thirty day MIN  
            #8) 5 hour high, 9) 2.5 hour high,
            #10) 5 hour low, 11) 2.5 hour low,
            #12) Volume avg, 13) Volume
            
        except Exception as e:
            print(e)
    
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
