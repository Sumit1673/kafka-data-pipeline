import os
import psycopg2
from flask import Flask, request

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


@app.post('/vi/get-prices')
def get_distinct_prices():
    result = []
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT stock_name, DISTINCT(prices), time_stamp FROM SSEX limit 15')
    stocks = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    for row in stocks:
        result.append(dict(zip(columns, row)))
    cur.close()
    conn.close()
    return result

if __name__ == '__main__':
    print("Hello from data-api")
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=5000)