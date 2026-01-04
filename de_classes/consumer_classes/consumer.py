#author THO HUI YEE
import json
from typing import Any, Dict, List
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from .consumer_config import ConsumerConfig


class Consumer:
    def __init__(self, spark:SparkSession, cfg: ConsumerConfig) -> None:
        self.cfg = cfg
        self.spark = spark
        self.consumer = self._init_kafka_consumer()
        self.buffer: List[Dict[str, Any]] = []


    def _init_kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.cfg.topic,
            bootstrap_servers=self.cfg.bootstrap_servers,
            group_id=self.cfg.group_id,
            auto_offset_reset=self.cfg.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        )

    def _to_dataframe(self, records: List[Dict[str, Any]]) -> DataFrame:
        schema = getattr(self.cfg, "schema", None)
        if isinstance(schema, StructType):
            df = self.spark.createDataFrame(records, schema=schema)
        else:
            df = self.spark.createDataFrame(records)

        if self.cfg.output_coalesce > 0:
            df = df.coalesce(self.cfg.output_coalesce)

        return df

    def _write_parquet(self, df: DataFrame) -> None:
        writer = (
            df.write
              .mode("append")
              .option("parquet.block.size", str(self.cfg.parquet_block_size))
        )
        writer.parquet(self.cfg.output_path)

    def _flush(self) -> None:
        if not self.buffer:
            return

        count = len(self.buffer)
        df = self._to_dataframe(self.buffer)
        self._write_parquet(df)
        self.consumer.commit()
        self.buffer.clear()

        print(f"[WRITE] extracted: {count} records → {self.cfg.output_path}")
        print("[COMMIT] offsets saved")

    def run(self) -> None:
        print(f"[INFO] start topic={self.cfg.topic}, group_id={self.cfg.group_id}")
        received_any = False
        
        try:
            while True:
                polled = self.consumer.poll(timeout_ms=1000, max_records=1000)
    
                if not polled:
                    if received_any:
                        print("[INFO] All messages received — finishing up.")
                        break
                    else:
                        continue
    
                for _, messages in polled.items():
                    for msg in messages:
                        if isinstance(msg.value, dict):
                            if self.cfg.verbose_receive:
                                self._log_received(msg.value)
                            

                            self.buffer.append(msg.value)
                            received_any = True
    
                        if len(self.buffer) >= self.cfg.batch_size:
                            self._flush()
    
        except KeyboardInterrupt:
            print("[INFO] Ctrl+C — final flush")
        finally:
            self._flush()
            self.consumer.close()
            print("[INFO] stopped cleanly")

    def _log_received(self, record: Dict[str, Any]) -> None:
        print(f"[RECEIVED] {record}")