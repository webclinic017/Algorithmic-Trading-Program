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
            
            if SP_INFO[i][0] == "CSCO" or SP_INFO[i][0] == "CAT":
                SP_CONTRACTS[i].exchange = "ISLAND" 
            else:
                SP_CONTRACTS[i].exchange = 'SMART'
                
            SP_CONTRACTS[i].symbol = SP_INFO[i][0]
            
            #Request to stream ticker prices
            self.reqMktData(i, SP_CONTRACTS[i], '', False, False, [])
            
            if SP_INFO[i][0] == "CSCO" or SP_INFO[i][0] == "CAT":
                SP_CONTRACTS[i].exchange = 'SMART'
            print("REQUEST DATA", SP_INFO[i][0])
            #50 requests allowed per second    
            sleep(0.03)

    def tickPrice(self, reqId, tickType, price, attrib):
        
        #Checks if market is in open hours
        hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60
        if(hours > 16):    
            self.disconnect()
            
        if tickType == 66:
            global SP_INFO
            global SP_CONTRACTS  
            
            today = datetime.now()
            
            if price != -1:
                SP_INFO[reqId][3] = price
                SP_INFO[reqId][4] = str(today) 
            
            #0) SYMBOL
            #1) POSITION, 2) PRICE BOUGHT AT
            #3) CURRENT PRICE, 4) LAST UPDATED, 
            #5) ATR, 6) forty DAY MAX, 7) PREVIOUS CLOSE  
            #8) 5 hour hour high, 9) 2.5 hour high, 10) date
            
            if (hours >= 9.5 and hours < 16):
                global SP_CONTRACTS

                if(SP_INFO[reqId][1] > 0): #Exit strategy
                    if price >= SP_INFO[reqId][2] - SP_INFO[reqId][5]: #Stop loss
                        
                        #Close position
                        order = Order()
                        order.action = 'SELL'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                        
                        
                        SP_INFO[reqId][1] = 0 #Position is 0
                        print("EXIT, UNPROFITABLE")
                        print("Symbol: ", SP_INFO[reqId][0], "Quantity: ", order.totalQuantity, "Loss:", (SP_INFO[reqId][1])*(SP_INFO[reqId][3] - SP_INFO[reqId][2]),  "Time: ", datetime.now(), "\n")   
                        self.nextOrderID += 1
                    
                    elif price >= SP_INFO[reqId][2] and SP_INFO[reqId][8] > SP_INFO[reqId][9]: #Cash out
                        
                        #Close position
                        order = Order()
                        order.action = 'SELL'
                        order.totalQuantity = SP_INFO[reqId][1]
                        order.orderType = 'MKT'
                        self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)
                         
                        
                        SP_INFO[reqId][1] = 0 #Position is 0
                        print("EXIT, PROFITABLE")
                        print("Symbol: ", SP_INFO[reqId][0], "Quantity: ", order.totalQuantity, "Gain:", (SP_INFO[reqId][1])*(SP_INFO[reqId][3] - SP_INFO[reqId][2]),  "Time: ", datetime.now(), "\n")   
                        self.nextOrderID += 1

                elif(SP_INFO[reqId][1] < 1): #Entry Strategy
                    if price >= SP_INFO[reqId][6]:
                        print("ENTRY")
                        global CASH
                        if CASH//SP_INFO[reqId][3] > 0:
                            
                            CASH//SP_INFO[reqId][3]
                            order = Order()
                            order.action = 'BUY'
                            order.totalQuantity = max(1, (CASH*0.02)//(SP_INFO[reqId][3]) )
                            order.orderType = 'MKT'
                            self.placeOrder(self.nextOrderID, SP_CONTRACTS[reqId], order)                            
                            print("ENTRY")
                            print("Symbol: ", SP_INFO[reqId][0], "Quantity: ", order.totalQuantity, "Time: ", datetime.now(), "\n")   
                            self.nextOrderID += 1
                            
    def __init__(self): #ONE
        EClient.__init__(self, self)
        self.nextOrderID = 0

    def nextValidId(self, orderId): #TWO
        self.nextOrderID = orderId
        self.start()


class accountConnection(EWrapper, EClient):#Cash balance and positions
    def start(self):
        #Requests 
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
        
        if (contract.secType == "STK"):
            global SP_INFO
            global SP_INDEX
            SP_INFO[SP_INDEX[contract.symbol]][1] = position
            SP_INFO[SP_INDEX[contract.symbol]][2] = avgCost
        if (contract.secType == "CASH"):
            
            #US cash shows up once per cycle
            #Continues if market is in opening hours
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60            
            if (hours < 16): 
                time.sleep(15)
                self.reqAccountSummary(21, "All", "$LEDGER:ALL")
            elif hours >= 16:
                self.disconnect()             

    def accountSummary(self, reqId, account, tag, value, currency):#FOUR
        if (tag == "CashBalance" and currency == "USD"):        
            global CASH
            CASH = value
            
            #Continues if market is in opening hours
            hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60
            if (hours < 16): 
                time.sleep(15)
                self.reqPositions()
            elif hours >= 16:
                self.reqPositions()
                self.disconnect()               

def tickerThread(): 
    global tickerSocket
    tickerSocket.run()
       
def accountThread():
    global accountSocket
    accountSocket.run()
    
def highsLowsThread():   
    global SP_INFO 
    hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60
    
    #Quit when market closes
    while(hours < 16):
        for i in SP_INFO:
            
            #Only start requesting data once market opens
            if hours >= 9.5:                
                data = yf.download(tickers= SP_INFO[i][0], period="2d", interval="5m") 
                
                max5h = data.at[data.index[len(data.index) - 60], 'High']
                min5h = data.at[data.index[len(data.index) - 60], 'Low']
                
                vol = 0 
                
                max2h30m = data.at[data.index[len(data.index) - 30], 'High']
                min2h30m = data.at[data.index[len(data.index) - 30], 'Low']              

                #Get 6 hour high and low
                for x in range(len(data.index) - 72, len(data.index)):
                    
                    max5h = max(max5h, data.at[data.index[x], 'High'])
                    min5h = min(min5h, data.at[data.index[x], 'Low'])
                    vol = vol + data.at[data.index[x], 'Volume']
                    
                    #Get 2.5 hour high and low
                    if x > len(data.index) - 30:
                        max2h30m = max(max2h30m, data.at[data.index[x], 'High'])
                        min2h30m = min(min2h30m, data.at[data.index[x], 'Low'])                    
    
                SP_INFO[i][8] = max5h
                SP_INFO[i][9] = max2h30m
                SP_INFO[i][10] = vol/72
                SP_INFO[i][11] = data.at[data.index[len(data.index) - 1], 'Volume']
        
        time.sleep(240)
        hours = float(str(datetime.now())[11:13]) + float(str(datetime.now())[11:13])/60

def main():
    
    today = datetime.now()
    startDate = str(today - timedelta(days=40))[0:10]
    endDate = str(today)[0:10]
    
    global SP_INFO
    SP_INFO ={} 
    
    global SP_INDEX
    SP_INDEX ={}  
    
    i = 0
    
    #Read list of stocks to be traded
    tsx_list = open("test_stocks.txt", "r")    
    
    for x in tsx_list:        
        symbol = x.strip()
        i = i + 1
        try:
            #Get pandas dataframe of stock info over last days
            data = web.DataReader(symbol, 'yahoo', startDate, endDate)
            
            
            #Get 10 day ATR
            atr = 0
            for j in range(len(data.index) - 10, len(data.index)):
                a = data.at[data.index[j], 'High'] - data.at[data.index[j], 'Low']
                b = abs(data.at[data.index[j], 'High'] - data.at[data.index[j - 1], 'Close'])
                c = abs(data.at[data.index[j], 'Low'] - data.at[data.index[j - 1], 'Close'])
                atr = atr + max(a, b, c)
                previous = data.at[data.index[j - 1], 'Close']
                
            #Get max over last 40 days
            fortydaymax = 0
            for j in range(0, len(data.index)):
                fortydaymax = max(data.at[data.index[j], 'High'], fortydaymax)
                
            SP_INFO[i] = [symbol, -1, -1, -1, "NULL", atr/10, fortydaymax, previous, -1, -1, -1, -1] 
            SP_INDEX[symbol] = i
            
            #0) SYMBOL
            #1) POSITION, 2) PRICE BOUGHT AT
            #3) CURRENT PRICE, 4) LAST UPDATED, 
            #5) ATR, 6) forty DAY MAX, 7) PREVIOUS CLOSE  
            #8) 5 hour high, 9) 2.5 hour high, 10) Volume avg, 11) Volume
            
        except Exception as e:
            print(e)
    
    global tickerSocket
    global accountSocket
    
    tickerSocket = tickerConnection()
    accountSocket = accountConnection()  
    
    tickerSocket.connect('127.0.0.1', 7497, 0)
    accountSocket.connect('127.0.0.1', 7497, 1)
    
    sleep(1)
    
    TT = Thread(target=tickerThread, daemon=True) #Summary and positions
    AT = Thread(target=accountThread, daemon=True) #Tickers
    HLT = Thread(target=highsLowsThread, daemon=True)#Highs and Lows for last 5 hours  
    
    TT.start()
    AT.start()
    HLT.start() 
    
    TT.join()
    AT.join()
    HLT.join()
    
    tickerSocket.disconnect()
    accountSocket.disconnect()    
    print("\n")
    print("Ended Trading")
     
main()
