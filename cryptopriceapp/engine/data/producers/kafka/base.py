import os
import socket
import logging
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient
from confluent_kafka.avro import AvroProducer


class BaseProducer:
    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema,
            partitions=1,
            replication=1
    ):
        # init logger
        self.logger = logging.getLogger("engine.producers.kafka.base.BaseProducer")

        # initialize object variables
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.partitions = partitions
        self.replication = replication

        # initialize kafka broker properties
        self.broker_properties = {
            "bootstrap.servers": [os.getenv("KAFKA_BROKER1"), os.getenv("KAFKA_BROKER2")],
            "schema.registry.url": f"{os.getenv('KAFKA_SCHEMA_REGISTRY')}",
            "client.id": socket.gethostname()
        }

        # initialize default avro producer
        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

        # init kafka admin client object to communicate with kafka backend
        self.admin_client = AdminClient(conf={"bootstrap.servers": self.broker_properties['bootstrap.servers']})

        # create topic name if it doesn't exist
        self.create_topic()

    def create_topic(self):
        """
        This creates a kafka topic using the self.topic_name if the topic doesn't exist already
        :return:
        """
        if self._does_topic_exist(self.topic_name):
            self.logger.info(f"WARNING: {self.topic_name} already exists.Skipping creating")
            return

        # create topic using the NewTopic object
        topic = NewTopic(
            self.topic_name,
            num_partitions=self.partitions,
            replication_factor=self.replication
        )

        # use admin client to create topic
        topic_futures = self.admin_client.create_topics([topic])

        for topic, future in topic_futures.items():
            try:
                future.result()
                self.logger.debug(f"SUCCESS: {self.topic_name} created successfully")
            except Exception as e:
                self.logger.debug(f"ERROR: {self.topic_name} could not be created.")
                raise e

    def close(self):
        """
        This flushes any existing producer and closes it
        :return:
        """
        if not self.producer:
            return
        self.producer.flush()
        self.logger.debug("SUCCESS: Producer close successfully")

    def _does_topic_exist(self, topic_name):
        pass
