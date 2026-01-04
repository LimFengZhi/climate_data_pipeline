#author LIMFENGZHI
from .streamer import Streamer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateRiskScoringStreamer(Streamer):
    def __init__(self, spark: SparkSession, stream_file_path):
        super().__init__(spark, stream_file_path) 

    def _operate_stream(self):
        logger.info("Starting Stream: Climate Risk Scoring")
        self.data_stream = self._start_streaming(data_schema = self.data_schema, file_path = self.stream_file_path)
        activity = (self.data_stream.withColumn("date", F.to_timestamp(F.col("date").cast("string"), "yyyy-MM-dd")))
        
        scored = (activity.join(F.broadcast(self.table_city), on="city_name", how="left").fillna({"country": "Unknown"}))

        query_grouped = (scored.groupBy(
            F.col("country")
                 ) \
                 .agg(
                     F.round(F.avg("climate_risk_score"), 2).alias("avg_risk_score"),
                     F.max("climate_risk_score").alias("max_risk_score"),
                     F.min("climate_risk_score").alias("min_risk_score"),
                     F.sum(F.when(F.col("climate_risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_cnt"),
                     F.sum(F.when(F.col("climate_risk_level") == "HIGH", 1).otherwise(0)).alias("high_cnt"),
                     F.sum(F.when(F.col("climate_risk_level") == "MODERATE", 1).otherwise(0)).alias("moderate_cnt"),
                     F.sum(F.when(F.col("climate_risk_level") == "LOW", 1).otherwise(0)).alias("low_cnt"), 
                     F.count("*").alias("events")
                 ) \
            ).select(
                     "country",
                     "avg_risk_score", "max_risk_score", "min_risk_score",
                     "low_cnt", "moderate_cnt", "high_cnt", "critical_cnt", "events"
                 )
        
        def pct(n, d):
            return F.round(F.when(d > 0, (n.cast("double")/d)*100.0).otherwise(0.0),2)
            
        query_pct_analysis = (query_grouped
                 .withColumn("pct_low",       pct(F.col("low_cnt"),      F.col("events")))
                 .withColumn("pct_moderate",  pct(F.col("moderate_cnt"), F.col("events")))
                 .withColumn("pct_high",      pct(F.col("high_cnt"),     F.col("events")))
                 .withColumn("pct_critical",  pct(F.col("critical_cnt"), F.col("events")))
                 .withColumn("pct_worst",
                             pct(F.col("high_cnt") + F.col("critical_cnt"), F.col("events")))
                 
        )
        
        query_climate_analysis = (query_pct_analysis
                 .withColumn("high_risk_days_ratio",(F.round((F.col("high_cnt")+ F.col("critical_cnt")) / F.col("events"), 2)))
                 .withColumn("climate_vulnerability_index",
                            F.round(
                                (F.col("high_risk_days_ratio") * 0.6) + 
                                (F.col("avg_risk_score") / 10 * 0.4),
                                2)
                            )
                 .withColumn("vulnerability_category",
                             F.when(F.col("climate_vulnerability_index") > 0.7, "EXTREME")
                             .when(F.col("climate_vulnerability_index") > 0.5, "HIGH")
                             .when(F.col("climate_vulnerability_index") > 0.3, "MODERATE")
                             .otherwise("LOW")
                            )
                )
        
        return query_climate_analysis

    def process_stream(self):
        query = None
        try:
            query = self._write_stream(query_name = "climate_risk_score", outputMode = "update", truncate = "false", checkpoint = os.getenv("CHECK_POINT_RISK_SCORE"), trigger_processing_time =20)
            
            query.awaitTermination() 
            if query.exception() is not None:
                logger.info("Query failed with exception:", f"{query.exception()}")

        except KeyboardInterrupt:
            logger.info("Stopping all streams...")
            if query is not None and query.isActive:
                query.stop()
        except Exception as e:
            logger.error(f"Stream failed with error: {e}")
            if query is not None and query.isActive:
                query.stop()
        finally:
            pass
    