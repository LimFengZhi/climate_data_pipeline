#author LIMFENGZHI
from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, app_name = "SparkApp", configs : dict = None):
        self.app_name = app_name
        self.configs = configs or {}
        self._spark = None

    def __enter__(self):
        spark = SparkSession.builder.appName(self.app_name)
        spark.config(map = self.configs)
        self._spark = spark.getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._spark.stop()

    @property
    def spark(self):
        return self._spark