import os
import tweepy
from twython import TwythonStreamer
from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers='localhost:9092')
# topic = "crypto"


class TweetStreamer(tweepy.Stream):
    def on_data(self, raw_data):
        print(raw_data)


class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        print(data)

    def on_error(self, status_code, data, headers=None):
        print(status_code)


def start_stream():
    tweet_stream = TweetStreamer(os.environ['TWITTER_API_KEY'],
                                 os.environ['TWITTER_API_KEY_SECRET'],
                                 os.environ['TWITTER_ACCESS_TOKEN'],
                                 os.environ['TWITTER_ACCESS_TOKEN_SECRET'])

    stream = MyStreamer(os.environ['TWITTER_API_KEY'],
                        os.environ['TWITTER_API_KEY_SECRET'],
                        os.environ['TWITTER_ACCESS_TOKEN'],
                        os.environ['TWITTER_ACCESS_TOKEN_SECRET'])

    stream.statuses.filter(track='$btc')

    # print(tweet_stream.sample())

