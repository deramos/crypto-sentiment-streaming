from pyspark import SparkContext


class StreamProcessor:
    def __init__(self):
        self.server = "local[*]"

    def process_stream(self):
        sc = SparkContext(self.server)
