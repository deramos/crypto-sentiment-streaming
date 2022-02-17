from pyspark import SparkContext


class SparkStreamProcessor:
    def __init__(self):
        self.server = "local[*]"

    def process_stream(self):
        sc = SparkContext(self.server)
