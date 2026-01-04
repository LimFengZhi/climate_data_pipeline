#author: THO HUI YEE
from de_classes.consumer_classes.carbon_consumer import CarbonConsumer
from de_classes.consumer_classes.consumer_config import ConsumerConfig
from de_classes.utility_classes.spark_manager import SparkManager
from dotenv import load_dotenv
import os
from pathlib import Path

def main():
    load_dotenv()
    OUTPUT_PATH = os.getenv('RAW_STREAMED_CO2')

    with SparkManager(app_name = "Consumer Spark") as manager:
        co2_spark = manager.spark
        co2_cfg = ConsumerConfig(
            topic="co2_data",
            group_id="co2_data_group",
            output_path= OUTPUT_PATH,
            batch_size=1000,
            output_coalesce=1,
            parquet_block_size=64*1024*1024,
            verbose_receive=True,
            schema = CarbonConsumer.get_schema()
        )
        
        consumer = CarbonConsumer(spark = co2_spark, cfg = co2_cfg)
        consumer.run()
    
if __name__ == "__main__":
    main()