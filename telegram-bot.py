import datetime
import pickle as pkl
import logging
import os
import time

from dotenv import load_dotenv
# from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from db import *

trophies = {0: '🥇', 1: '🥈', 2: '🥉'}

# Load enviroment variables from the .env file
load_dotenv()
TOKEN = os.getenv('TOKEN')
GROUP_ID = os.getenv('GROUP_ID')
LOGGER_FILE = os.getenv('LOGGER_FILE')
DB_NAME = os.getenv('DB_NAME')
MAX_VAL_PATH = os.getenv('MAX_VAL')
MINUTES_BROADCAST = int(os.getenv('MINUTES_BROADCAST'))

# Load database
conn = conn_db(DB_NAME, ro=True)
c = conn.cursor()


def get_prices(df):
    """
    Get price of UST in ARS from many exchanges

    Parameters
    ----------
    df : pd.DataFrame
        Timeseries DataFrame with data

    Returns
    -------
    str
        A ready message to send in Telegram
    """
    try:
        created_time = df.date.max() - datetime.timedelta(minutes=2)
        top3 = df[df['date'] > created_time].drop_duplicates(
            subset=['exchange']).nlargest(3, 'totalBid')
        exchanges_list = list(top3['exchange'])
        bids_list = list(top3['totalBid'])

        # TODO: Compare vs last price in %
        messages = [
            '{} {} : {:.2f}ARS'.format(trophies[i], exchanges_list[i].title(),
                                       bids_list[i]) for i in range(3)
        ]
        final_message = '\n'.join(messages)
        return final_message
    except Exception as e:
        return 'error' + e


def get_max_value(df):
    """
    Get the max value of the last 100 rows

    Parameters
    ----------
    df : pd.DataFrame
        Timeseries DataFrame with data

    Returns
    -------
    dict
        Dictionary with the max value and their values
    """
    return df.iloc[df['totalBid'].argmax()].to_dict()


# def start(update, context):
#     update.message.reply_text(get_prices(df))

# # Create a function to send messages automaticaly every minute
# def send_message(update, context):
#     update.message.reply_text(get_prices(df))


def broadcast_msg(msg):
    to_url = 'https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}&parse_mode=HTML'.format(
        TOKEN, GROUP_ID, msg)
    resp = requests.get(to_url)
    # print(resp.text)


class Broadcaster:
    def __init__(self, logger):
        self.max_value = {'totalBid': 0}
        self.max_path = MAX_VAL_PATH
        self.logger = logger

    def set_max_value(self):
        """
        Set the max value from the file
        """
        if os.path.exists(self.max_path):
            with open(self.max_path, 'rb') as f:
                self.max_value = pkl.load(f)

    def check_max_value(self, max_value):
        """
        Check if the max value is new

        Parameters
        ----------
        max_value : dict
            Max value from the dataframe
        """
        if max_value.get('totalBid') > self.max_value.get('totalBid'):
            self.max_value = max_value
            with open(self.max_path, 'wb') as f:
                del max_value['date']
                pkl.dump(self.max_value, f)
            return True
        else:
            return False

    def start(self):
        while True:
            self.logger.info('Retrieving data')
            df = get_dataframe(c)

            self.logger.info('Check max value')
            max_value = get_max_value(df)
            
            if self.check_max_value(max_value):
                broadcast_msg(
                    '🚨🤑💲 Nuevo valor máximo histórico en {}: {:.2f}ARS'.format(
                        max_value['exchange'].title(), max_value['totalBid']))

            self.logger.info('Broadcasting')
            broadcast_msg(get_prices(df))

            self.logger.info('Sleeping')
            time.sleep(MINUTES_BROADCAST * 60)


if __name__ == '__main__':
    # updater = Updater(token=TOKEN, use_context=True)
    # add handler
    # updater.start_polling()
    # updater.idle()

    # dp.add_handler(MessageHandler(Filters.text, send_message))
    # dp = updater.dispatcher

    logging.basicConfig(filename=LOGGER_FILE,
                        level=logging.DEBUG,
                        filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('ts_crypto')

    # Create a broadcaster
    broadcaster = Broadcaster(logger)

    # Start the broadcaster
    broadcaster.start()
