#author THO HUI YEE
from .consumer import Consumer
from .consumer_config import ConsumerConfig
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from typing import Dict, Any

class CarbonConsumer(Consumer):
    
    @staticmethod
    def get_schema() -> StructType:
        return StructType([
            StructField("country", StringType(), True),
            StructField("date", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("MtCO2_per_day", DoubleType(), True)
        ])

    def _log_received(self, record: Dict[str, Any]) -> None:
        country = record.get("country", "Unknown")
        date = record.get("date", "Unknown")
        sector = record.get("sector", "Unknown")
        print(f"[RECEIVED] CO2: {country} — {sector} — {date}")