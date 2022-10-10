from kafka import KafkaProducer
import json
import random
import time
import copy
import os

BOOTSTRAP_SERVER_URL = os.environ.get("BOOTSTRAP_SERVER_URL", "kafka:9092")


class DataGenerator:
    """
    this class is used to generate data to be consumed by KAFKA. It takes a json file
    which includes the lists of stocks and their prices. This json file can be usd to
    add more stocks if needed.
    """
    def __init__(self) -> None:
        try:
            self.producer = KafkaProducer(
                bootstrap_servers="kafka:9092", # Kafka brokers to fetch the initial metadata about the kafka clusters
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))

            with open('stocks.json') as json_fd:
                self.stocks_json_data = json.load(json_fd)
        except Exception as e:
            print(e)
            exit()

    @staticmethod
    def calculate_mean_price(stock):
        new_stock = {}
        price = stock["price"]
        pos_price = int(price + (10 * price) / 100.0)
        neg_price = int(price - (10 * price) / 100.0)

        new_stock["price"] = int(random.uniform(neg_price, pos_price))
        new_stock["name"] = stock["name"]
        return new_stock

    def generate_data(self):
        """
        This function has 3 capabilities:
            1. Select a random number e.g. 'x', between the 1 and total number of stocks
            2. Create a mean price (+-10%) of 'x' number of stocks
            3. Produce the data to sent to Kafka
        """
        portfolio_len = len(self.stocks_json_data["ticker"])

        while 1:
            stock_json_message = {}
            ticker_data = []

            end_time = time.time() + 1

            while time.time() < end_time:
                if len(ticker_data) >= 10:
                    continue

                # extract x number of stocks
                n_stocks = random.choice(range(1, portfolio_len))
                for _ in range(n_stocks):
                    stock_index = random.randint(0, portfolio_len-1)
                    stock = self.calculate_mean_price(self.stocks_json_data["ticker"][stock_index])
                    ticker_data.append(stock)

            stock_json_message["tickers"] = copy.deepcopy(ticker_data)
            print("Message Sent")
            self.producer.send('stocks', stock_json_message)
            time.sleep(1)


if __name__ == '__main__':
    data_generator = DataGenerator()
    data_generator.generate_data()