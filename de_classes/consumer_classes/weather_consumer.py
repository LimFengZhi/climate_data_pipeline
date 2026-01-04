#author THO HUI YEE
from .consumer import Consumer
from .consumer_config import ConsumerConfig
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from typing import Dict, Any


class WeatherConsumer(Consumer):
    
    @staticmethod
    def get_schema() -> StructType:
        return StructType([
            StructField("station_id", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("season", StringType(), True),
            StructField("avg_temp_c", DoubleType(), True),
            StructField("min_temp_c", DoubleType(), True),
            StructField("max_temp_c", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("snow_depth_mm", DoubleType(), True),
            StructField("avg_wind_dir_deg", DoubleType(), True),
            StructField("avg_wind_speed_kmh", DoubleType(), True),
            StructField("peak_wind_gust_kmh", DoubleType(), True),
            StructField("avg_sea_level_pres_hpa", DoubleType(), True),
            StructField("sunshine_total_min", DoubleType(), True),
        ])

    def _log_received(self, record: Dict[str, Any]) -> None:
        city = record.get("city_name", "Unknown")
        date = record.get("date", "Unknown")
        print(f"[RECEIVED] Weather: {city} — {date}")