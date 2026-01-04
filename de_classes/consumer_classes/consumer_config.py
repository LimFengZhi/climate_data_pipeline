#author THO HUI YEE
from typing import Optional
from pyspark.sql.types import StructType


class ConsumerConfig:
    def __init__(
        self,
        topic: str,
        group_id: str,
        output_path: str,
        *,
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "earliest",
        batch_size: int = 500,
        output_coalesce: int = 1,
        parquet_block_size: int = 64 * 1024 * 1024,
        spark_shuffle_partitions: str = "16",
        log_level: str = "ERROR",
        schema: Optional[StructType] = None,
        verbose_receive: bool = False,
    ):
        self.topic = topic
        self.group_id = group_id
        self.output_path = output_path
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self.batch_size = batch_size
        self.output_coalesce = output_coalesce
        self.parquet_block_size = parquet_block_size
        self.spark_shuffle_partitions = spark_shuffle_partitions
        self.log_level = log_level
        self.schema = schema
        self.verbose_receive = verbose_receive