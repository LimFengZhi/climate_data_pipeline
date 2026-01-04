#author LIMFENGZHI
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging
from .data_preprocessing_config import DataPreprocessingConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Preprocessor(ABC):
    def __init__(self, spark: SparkSession, input_path: str, output_path: str, config: DataPreprocessingConfig):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = spark
        self.df = None
        self.config = config
         
        
    def read_data(self):
        try:
            self.df = self.spark.read.parquet(self.input_path)
            logger.info(f"Data read successfully from {self.input_path}")
            logger.info(f"Initial row count: {self.df.count()}")
            logger.info(f"Schema: {self.df.printSchema()}")
        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise

    def add_processing_timestamp(self):
        try:
            self.df = self.df.withColumn('processing_time', F.current_timestamp())
            logger.info("Added 'processing_time' column with current timestamp")
        except Exception as e:
            logger.error(f"Error adding processing timestamp : {str(e)}")
            raise

    
    def remove_duplicates(self):
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates(self.config.primary_key_cols)
        final_count = self.df.count()
        logger.info(f"Duplicates removed: {initial_count - final_count} rows (based on {self.config.primary_key_cols})")

    def drop_columns(self):
        self.df = self.df.drop(*[c for c in self.config.remove_cols if c in self.df.columns])
        logger.info(f"Column removed: {self.config.remove_cols}")

    def cleaning_data(self):
        logger.info("Starting data cleaning process...")

        cleaning_rules = [
            (self.config.alpha_cols,r"[^a-zA-Z\s\-''·\.]", None, "alphabetic characters"),
            (self.config.integer_cols, r'[^0-9]', IntegerType(), "IntegerType"),
            (self.config.double_cols, r'[^0-9.-]', DoubleType(), "DoubleType")
        ]

        for columns, pattern, cast_type, msg in cleaning_rules:
            if columns:
                for col_name in columns:
                    if col_name in self.df.columns:
                        cleaned_col = F.regexp_replace(F.col(col_name), pattern, '')
                        if cast_type:
                            if cast_type == DoubleType():
                                self.df = self.df.withColumn(col_name, cleaned_col.cast(cast_type))
                                logger.info(f"Cleaned and casted column '{col_name}' to {msg}.")
                            if cast_type == IntegerType():
                                self.df = self.df.withColumn(col_name, cleaned_col.cast(cast_type))
                                logger.info(f"Cleaned and casted column '{col_name}' to {msg}.")
                        else:
                            self.df = self.df.withColumn(col_name, cleaned_col)
                            logger.info(f"Cleaned column '{col_name}' to contain only {msg}.")

    def handle_missing_values(self):
        if self.config.handle_remove_value_cols:
            logger.info("Dropping rows with missing values for specified columns.")
            self.df = self.df.na.drop(how = 'any', subset = self.config.handle_remove_value_cols)
    
                    
    @abstractmethod
    def standardize_data(self):
        pass
    
    @abstractmethod
    def enrich_data(self):
        pass
    
    @abstractmethod
    def validate_data(self, referencial_table = None):
        pass
    
    
    def write_data(self, write_mode):
        try:              
            self.df.coalesce(10).write \
                .mode(write_mode) \
                .option("compression", "gzip") \
                .parquet(self.output_path)
            logger.info(f"Data written successfully to {self.output_path}")
            logger.info(f"Final row count: {self.df.count()}")
        except Exception as e:
            logger.error(f"Error writing data: {str(e)}")
            raise

    def process(self, referential_check_dataset = None, write_mode = "append"):
        logger.info(f"Starting {self.__name__} processing pipeline...")
        
        self.read_data()
        self.add_processing_timestamp()

        logger.info("Droping columns...")
        self.drop_columns()
        
        logger.info("Starting data cleaning...")
        self.cleaning_data()
        self.remove_duplicates()
        
        logger.info("Handling missing values...")
        self.handle_missing_values()
        
        logger.info("Standardizing data...")
        self.standardize_data()

        logger.info("Enriching data...")
        self.enrich_data()

        logger.info("Validating data...")
        validation_report = self.validate_data(referencial_table = referential_check_dataset)
        
        self.write_data(write_mode)
        
        logger.info(f"{self.__name__} processing pipeline completed successfully!")
        return validation_report