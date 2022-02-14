import logging
from pathlib import Path
from crytanalyzer.config.config import CASSANDRA_KEYSPACE
from cassandra.cluster import Cluster

logger = logging.getLogger("Cassandra init")


def init():
    cluster = Cluster()

    try:
        session = cluster.connect(CASSANDRA_KEYSPACE)
    except Exception as e:
        logger.warning("Error connecting to {} keyspace {}. Creating Keyspace... ".format(CASSANDRA_KEYSPACE, e))
        session = cluster.connect()
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} WITH replication = "
                        "{'class': 'SimpleStrategy', 'replication_factor': '3'};")

        session = cluster.connect(CASSANDRA_KEYSPACE)

    # read init.sql file
    with open(Path.joinpath(Path(__file__).parent, "sql/init.sql"), 'r') as file:
        sql = file.read()

    # execute init.sql file
    session.execute(sql)
