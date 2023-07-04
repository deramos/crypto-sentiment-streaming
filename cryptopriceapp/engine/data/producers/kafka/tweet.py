import os
from pathlib import Path
import logging
from .base import BaseProducer
from confluent_kafka import avro


class TweetProducer(BaseProducer):
    def __init__(
            self,
            topic_name,
            partitions,
            replication
    ):
        # init logger
        self.logger = logging.getLogger("engine.data.producers.kafka.tweet.TweetProducer")

        # read avro key and schema from schema folder
        key_schema = avro.load(Path.joinpath(Path(__file__).parents[2], 'schemas/raw-tweet-key-schema.json'))
        value_schema = avro.load(Path.joinpath(Path(__file__).parents[2], 'schemas/raw-tweet-value-schema.json'))

        # call super init
        super(TweetProducer, self).__init__(
            topic_name,
            key_schema,
            value_schema,
            partitions,
            replication
        )
