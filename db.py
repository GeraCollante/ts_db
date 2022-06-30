import sqlite3
from datetime import datetime as dt
from statistics import fmean

import pandas as pd
import requests

from db import *

DB_NAME = 'ts_crypto.db'

def conn_db(db_name, ro=False):
    """
    Connect to the database

    Returns
    -------
    conn : sqlite3.Connection
        Connection to the database
    """
    if ro:
        db_name = 'file:' + db_name + '?mode=ro'
        return sqlite3.connect(db_name, uri=True)
    else:
        return sqlite3.connect(db_name)


def create_table(c, table_name, cols):
    """
    Create the database

    Parameters
    ----------
    c : sqlite3.Cursor
        Cursor to the database
    """
    columns = ', '.join([str(k) + ' ' + v for k,v in cols.items()])
    
    create_table_query = f"""
        CREATE TABLE {table_name}
                (
                    row INTEGER PRIMARY KEY AUTOINCREMENT,
                    {columns}
                );
    """

    c.execute(create_table_query)


def insert_row(c, values):
    """
    Insert a row into the database

    Parameters
    ----------
    c : sqlite3.Cursor
        Cursor to the database
    values : dict
        Dictionary of values to insert
    """
    c.execute(
        """INSERT INTO timeseries (time, exchange, coin, ask, totalAsk, bid, totalBid)
       VALUES (:time, :exchange, :coin, :ask, :totalAsk, :bid, :totalBid);""",
        values)


def get_dataframe(c):
    """
    Get the data from the database

    Parameters
    ----------
    c : sqlite3.Cursor
        Cursor to the database

    Returns
    -------
    df : pandas.DataFrame
        Dataframe with the data from the database
    """
    query = c.execute('SELECT * FROM timeseries ORDER BY time DESC LIMIT 100')

    # Get column names
    cols = [col[0] for col in query.description]
    # Create dataframe
    df = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    # Add column with formatted time
    df['date'] = df['time'].apply(lambda x: pd.Timestamp(
        x, unit='s', tz='America/Argentina/Buenos_Aires'))
    return df


def get_tables(c):
    """
    Get the tables from the database

    Parameters
    ----------
    c : sqlite3.Cursor
        Cursor to the database

    Returns
    -------
    tables : list
        List of tables in the database
    """
    tables = c.execute('SELECT name FROM sqlite_master WHERE type="table"')
    return [table[0] for table in tables]


def query_binance(c):
    """
    Query the Binance API and insert the data into the database

    Parameters
    ----------
    c : sqlite3.Cursor
        Cursor to the database
    """
    sell = 'https://criptoya.com/api/binancep2p/sell/usdt/ars/5'
    buy = 'https://criptoya.com/api/binancep2p/buy/usdt/ars/5'
    bid = fmean([
        float(adv['adv']['price']) for adv in requests.get(sell).json()['data']
    ])
    ask = fmean([
        float(adv['adv']['price']) for adv in requests.get(buy).json()['data']
    ])

    values = {}

    values['exchange'] = 'binance'
    values['coin'] = 'USDT'
    values['bid'] = bid
    values['totalBid'] = bid
    values['ask'] = ask
    values['totalAsk'] = ask
    values['time'] = dt.now().timestamp()
    insert_row(c, values)


def query_exchange(c, url):
    """
    Query the exchange API and insert the data into the database

    Parameters
    ----------
    url : str
        URL to the exchange API
    c : sqlite3.Cursor
        Cursor to the database
    """

    exchange = url.split('/')[4]
    coin = 'USDT'
    values = requests.get(url).json()
    values['exchange'] = exchange
    values['coin'] = coin
    insert_row(c, values)

