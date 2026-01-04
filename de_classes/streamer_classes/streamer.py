#author LIMFENGZHI
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from datetime import datetime

class Streamer(ABC):

    def __init__(self, spark: SparkSession, stream_file_path):
        self.spark = spark
        self.table_city = self._get_city_table()
        self.stream_file_path = stream_file_path
        self.stream_data = self.read_data(self.stream_file_path)
        self.data_schema = self.stream_data.schema
        self.data_stream = None

    def _get_city_table(self):
        df_city = self.spark.read.csv(os.getenv("CITY_CSV"), header = True)
        df_city = df_city.dropna(subset=["city_name"])
        df_city = df_city.dropDuplicates(['city_name'])
        return df_city

    def read_data(self, path):
        df =  self.spark.read.parquet(path)
        return df

    def _start_streaming(self, data_schema, file_path, max_file_per_trigger = 1):
        return (self.spark.readStream.schema(data_schema).option("maxFilesPerTrigger", max_file_per_trigger).parquet(file_path))

    @abstractmethod
    def _operate_stream(self):
        pass
    
    def _write_stream(self, query_name: str, outputMode = "complete", numRows = 20, truncate = "false", checkpoint = None, trigger_processing_time = 10):
        stream_operation = self._operate_stream()
        query = stream_operation.writeStream.outputMode(outputMode) \
            .format("console") \
            .queryName(query_name)\
            .option("truncate", truncate) \
            .option("numRows", numRows) \
            .trigger(processingTime =f'{trigger_processing_time} seconds')
        if checkpoint:
            query.option("checkpointLocation", checkpoint)
        return query.start()

    @abstractmethod
    def process_stream(self):
        pass      