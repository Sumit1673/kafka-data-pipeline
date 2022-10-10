from datetime import datetime
from kafka import KafkaConsumer
from functools import reduce
from pytz import timezone
import json
import time
import os
import psycopg2

# Enable Debug logging with these lines
# import logging
# import sys
#
# logger = logging.getLogger('kafka')
# logger.addHandler(logging.StreamHandler(sys.stdout))
# logger.setLevel(logging.DEBUG)

BOOTSTRAP_SERVER_URL = os.environ.get("BOOTSTRAP_SERVER_URL", "127.0.0.1:9092")
POSTGRESQL_SERVER_URL = os.environ.get("POSTGRESQL_SERVER_URL", "127.0.0.1:5432")
POSTGRESQL_DATABASE_NAME = "tgam"
POSTGRESQL_DATABASE_USERNAME = "postgres"
POSTGRESQL_DATABASE_PASSWORD = "postgres"
print(f"Bootstrap Server URL {BOOTSTRAP_SERVER_URL}")

def create_table(conn):
    """ create tables in the PostgreSQL database"""



    query = """
    CREATE TABLE IF NOT EXISTS SSEX (
    s_index SERIAL PRIMARY KEY,
    stock_name VARCHAR(255) NOT NULL,
    stock_price INTEGER,
    time_stamp VARCHAR(255)
        )"""

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()


def establish_db_connection():
    """
    Connect to PostGreSQL DB
    """
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(
                    host="postgres",
                    database=POSTGRESQL_DATABASE_NAME,
                    user=POSTGRESQL_DATABASE_USERNAME,
                    password=POSTGRESQL_DATABASE_PASSWORD
                )
        print(conn)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print("Inside Establish Connection: {}".format(error))


def insert_data(conn, stock_name, avg_stock_price, time_stamp):
    """ insert a new vendor into the vendors table """
    
    query = """INSERT INTO ssex (stock_name, stock_price, time_stamp) VALUES (%s, %s, %s)"""
    records = (stock_name, int(avg_stock_price), time_stamp)
    print(time_stamp)

    # # create a new cursor. helps python to execute POstgreSQL command in a database session
    cur = conn.cursor()
    # execute the INSERT statement
    cur.execute(query, records)
    conn.commit()
    # close communication with the database
    cur.close()

    # return stock_id

def average(stock_values):
    return reduce(lambda a, b: a + b, stock_values) / len(stock_values)


def persist_stock_prices(company, stock_price):
    print(f'Persisting Data => Company: {company}, Stock Price: {stock_price}')


def aggregate_data():
    print("Executing method => aggregate_data ")
    consumer = KafkaConsumer('stocks',
                             bootstrap_servers=[BOOTSTRAP_SERVER_URL],
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))
    #print("Managed to get consumer ")
    # establish connection with the DB
    #eastern = timezone('US/Eastern')
    conn = establish_db_connection()
    create_table(conn)

    while True:
        time.sleep(2)
        stocks_price = {}
        print("Polling Data")
        msg_pack = consumer.poll(timeout_ms=1000)
        # Check if there are records, before proceeding.
        if msg_pack:
            for _, messages in msg_pack.items():
                for message in messages:
                    stock_messages = message.value
                    for stock_message in stock_messages["tickers"]:
                        if stock_message["name"] not in stocks_price:
                            stocks_price[stock_message["name"]] = [stock_message["price"]]
                        else:
                            stocks_price[stock_message["name"]].append(stock_message["price"])
            for s_name, s_price in stocks_price.items():
                dateTimeObj = datetime.now()
                time_stamp = dateTimeObj.strftime("%Y%m%d%H%M")
                # persist_stock_prices(s_name, )
                insert_data(conn, s_name, average(s_price), time_stamp)

            
        else:
            print("No new records found")


if __name__ == '__main__':
    print("Starting Data Aggregator")
    # aggregate_data()
    aggregate_data()
