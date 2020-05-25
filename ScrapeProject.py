#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 25 12:01:00 2020

@author: Rav
"""

import requests
import pandas as pd
import sqlite3
import csv
from sqlite3 import Error

"""
function to create the db if it doesn't already exist and name it TipData.db
this creates the DB in memory and establishes an active connection
"""
def create_connection(db_file): 
    conn = None                 
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as a:
        print(a)
    finally:
        if conn:
            conn.close()

"""
#This is a request made to the approrpiate website, taking the form as the header defined
#AKA this looks to pretend to be a user machine, to access the site
It then the data in to a csv, as the above stores the dataframe in index position 0 of a list
"""
def WebScraper(url):
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
            }
    r = requests.get(url, headers=header)

    TipTable = pd.read_html(r.text) 
    TipTable[0].to_csv('/Users/UserName/Documents/Project/Temp1.csv', index=False)


"""
This looks to insert the data from the csv in to the database:
"""
def AppendToDB(db_file):
    sqlLiteConnection = sqlite3.connect(db_file)
    cursor = sqlLiteConnection.cursor()

    with open('/Users/UserName/Documents/Project/Temp1.csv', 'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['Date'], i['Company Name'], i['Ticker'], i['Broker name'], i['Recommendation'],i['Price'], i['Old pricetarget'],i['New pricetarget'], i['Brokerchange']) for i in dr]

    cursor.executemany("INSERT INTO ShareCast (Date, 'Company Name', Ticker, 'Broker name', Recommendation, Price, 'Old pricetarget', 'New pricetarget', Brokerchange, InsertStamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'));", to_db)
    sqlLiteConnection.commit()
    sqlLiteConnection.close()



    
if __name__ == '__main__':
    create_connection(r"/Users/UserName/Documents/Project/TipData.db")
    WebScraper("https://www.sharecast.com/uk_shares/broker_views")
    AppendToDB("/Users/UserName/Documents/Project/TipData.db")








