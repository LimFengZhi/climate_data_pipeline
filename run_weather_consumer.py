#author: THO HUI YEE
from de_classes.consumer_classes.weather_consumer import WeatherConsumer
from de_classes.consumer_classes.consumer_config import ConsumerConfig
from de_classes.utility_classes.spark_manager import SparkManager
from dotenv import load_dotenv
import os
from pathlib import Path

def main():
    load_dotenv()
    OUTPUT_PATH = os.getenv('RAW_STREAMED_WEATHER')

    with SparkManager(app_name = "Consumer Spark") as manager:
        weather_spark = manager.spark
        weather_cfg = ConsumerConfig(
            topic="weather_data",
            group_id="weather_daily_group",
            output_path= OUTPUT_PATH,
            batch_size=1000,
            output_coalesce=1,
            parquet_block_size=64*1024*1024,
            verbose_receive=True,
            schema = WeatherConsumer.get_schema()
        )
    
        consumer = WeatherConsumer(spark = weather_spark, cfg = weather_cfg)
        consumer.run()
    
if __name__ == "__main__":
    main()