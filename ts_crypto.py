import argparse
import logging
import time

from db import *

# Load enviroment variables from the .env file
load_dotenv()
LOGGER_FILE = os.getenv('LOGGER_FILE')
DB_NAME = os.getenv('DB_NAME')

urls = [
    'https://criptoya.com/api/tiendacrypto/usdt/ars',
    'https://criptoya.com/api/buenbit/usdt/ars',
    'https://criptoya.com/api/decrypto/usdt/ars/1',
    'https://criptoya.com/api/satoshitango/usdt/ars',
    'https://criptoya.com/api/letsbit/usdt/ars/0.1',
    'https://criptoya.com/api/bitso/usdt/ars/500',
    # 'https://criptoya.com/api/ftx/usdt/ars/0.1',
    'https://criptoya.com/api/lemoncash/usdt',
    'https://criptoya.com/api/bitmonedero/usdt/ars/0.02',
    'https://criptoya.com/api/kriptonmarket/usdt/ars/100',
    'https://criptoya.com/api/latamex/usdt',
    'https://criptoya.com/api/copter/usdt/ars/0.1',
    'https://criptoya.com/api/belo/usdt/ars/0.5',
    'https://criptoya.com/api/fiwind/usdt/ars/0.1'
]

cols = {
    'exchange': 'TEXT',
    'coin': 'TEXT',
    'time': 'TIMESTAMP',
    'ask': 'SMALLMONEY',
    'totalAsk': 'SMALLMONEY',
    'bid': 'SMALLMONEY',
    'totalBid': 'SMALLMONEY'
}

MINUTES_DELAY = 5

if __name__ == '__main__':
    logging.basicConfig(filename=LOGGER_FILE,
                        level=logging.DEBUG,
                        filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger('ts_crypto')

    # Parse arguments
    parser = argparse.ArgumentParser(description='Flags.')
    parser.add_argument('-d',
                        '--db',
                        dest='create_db',
                        default=False,
                        help='Create DB')
    args = parser.parse_args()

    # Connect to the database
    conn = conn_db(DB_NAME)
    c = conn.cursor()
    logger.info('Connected to the database')

    # Create the table if it doesn't exist
    tables = get_tables(c)
    if 'timeseries' not in tables:
        logging.info('Create table')
        create_table(c, 'timeseries', cols)

    # Query the exchanges
    logger.info('Querying exchanges')
    for url in urls:
        query_exchange(c, url)
    query_binance(c)

    # Update the database
    conn.commit()

    # time.sleep(MINUTES_DELAY * 60)

    # df = get_dataframe(c)
    # print(df)

    # Close the connection
    conn.close()
    logging.info('Connection closed')
