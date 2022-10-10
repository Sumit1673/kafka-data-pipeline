import os
import re
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)
BOOTSTRAP_SERVER_URL = os.environ.get("BOOTSTRAP_SERVER_URL", "127.0.0.1:9092")
POSTGRESQL_SERVER_URL = os.environ.get("POSTGRESQL_SERVER_URL")
POSTGRESQL_DATABASE_NAME = "tgam"
POSTGRESQL_DATABASE_USERNAME = "postgres"
POSTGRESQL_DATABASE_PASSWORD = "postgres"


def get_db_connection():
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
        print("Connecting Flask to DB Error: {}".format(error))

def execute_query(query):
    result = []
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query)
    stocks = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    for row in stocks:
        result.append(dict(zip(columns, row)))
    cur.close()
    conn.close()
    return result

    
@app.route('/vi/get-prices', methods=['GET', 'POST'])
def get_distinct_prices():
    if request.method=="GET":
        query = "select distinct on (stock_price) stock_price, stock_name, time_stamp from SSEX"
        # query = 'SELECT DISTINCT ON (stock_price), stock_name, time_stamp FROM SSEX order by stock_name limit 15'
        return jsonify({"tickers": execute_query(query)})

@app.route('/vi/filter-prices', methods=['GET', 'POST'])
def filter_prices():
    if request.method=="GET":
        start_time = request.args.get("start_time")
        end_time = request.args.get("end_time")

        query = f"""
        SELECT stock_name, stock_price, time_stamp FROM SSEX where time_stamp >= timestamp {start_time}
        and time_stamp < timestamp {end_time};"""

        return execute_query(query)


if __name__ == '__main__':
    print("Hello from data-api")
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=5000)