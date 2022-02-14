import os
import logging
from pathlib import Path
from twython import TwythonStreamer
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = "crypto"

logger = logging.getLogger("Kafka Producer")

with open(Path.joinpath(Path(__file__).parent.parent, 'cashtags.txt'), 'r') as cf:
    coins = cf.read().splitlines()


class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        id_ = data.get('id')
        timestamp = data.get('created_at')
        text = data.get('text')

        producer.send(topic, value=f"{id_}\t{timestamp}\t{text}")

    def on_error(self, status_code, data, headers=None):
        logger.exception(status_code)


def start_stream():
    stream = MyStreamer(os.environ['TWITTER_API_KEY'],
                        os.environ['TWITTER_API_KEY_SECRET'],
                        os.environ['TWITTER_ACCESS_TOKEN'],
                        os.environ['TWITTER_ACCESS_TOKEN_SECRET'])

    stream.statuses.filter(track=coins)
