import os
import logging
import twython

from cryptopriceapp.engine.data.producers.kafka.tweet import TweetProducer

import dotenv

dotenv.load_dotenv('../../../../.env')


class TweetStreamClient(twython.TwythonStreamer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = logging.getLogger("engine.data.producers.external.tweet.TweetStreamClient")

        self.producer = TweetProducer(
            topic_name="bitcoin.tweets.raw",
            partitions=10,  # minimum 10 per topic
            replication=2  # two kafka servers
        )

        self.ticker_symbols = ['$BTC', '$ETH']  # modify to read from cashtags file

    def on_success(self, data):
        if 'text' in data:
            print(data)
            self.disconnect()

    def on_error(self, status_code, data, headers=None):
        self.logger.error(headers)


if __name__ == "__main__":
    client = TweetStreamClient(
        os.getenv('TWITTER_API_KEY'),
        os.getenv('TWITTER_API_KEY_SECRET'),
        os.getenv('TWITTER_ACCESS_TOKEN'),
        os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
    )

    client.statuses.filter(track=', '.join(client.ticker_symbols))
