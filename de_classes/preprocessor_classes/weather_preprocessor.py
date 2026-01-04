#author THAM ZHEN HERN, LIMFENGZHI
from .preprocessor import Preprocessor
from .data_preprocessing_config import DataPreprocessingConfig
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import logging
import functools
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherPreprocessor(Preprocessor):
    def __init__(self, spark: SparkSession, input_path: str, output_path: str, config: DataPreprocessingConfig):
        super().__init__(spark, input_path, output_path, config)
        self.__name__ = "Weather Processor"

    @staticmethod
    def _calculate_spi_index(df):
        date = F.to_date(F.col("date"))
        monthly_window = Window.partitionBy("station_id", F.year(date), F.month(date))
        station_window = Window.partitionBy("station_id")

        precipitation = F.col("precipitation_mm").cast("double")

        df = (df
              .withColumn("monthly_precip_avg", F.avg(precipitation).over(monthly_window))
              .withColumn("precip_mean", F.avg("monthly_precip_avg").over(station_window))
              .withColumn("precip_std", F.stddev("monthly_precip_avg").over(station_window))
              .withColumn(
                  "spi_index",
                  F.when(F.col("precip_std").isNull() | (F.col("precip_std") == 0), F.lit(None).cast("double"))
                   .otherwise((F.col("monthly_precip_avg") - F.col("precip_mean")) / F.col("precip_std"))
              )
              .withColumn("spi_index", F.round(F.col("spi_index"), 1))
              .drop("monthly_precip_avg", "precip_mean", "precip_std")
        )
        return df

    @staticmethod
    def _calculate_climate_risk_scoring(df):
        logger.info("Adding climate risk scoring to weather data")
        df = df.withColumn(
            "temp_extreme_score",
            F.when(F.col("max_temp_c")> 35, 3) \
            .when(F.col("max_temp_c") > 30, 2) \
            .when(F.col("min_temp_c") < -10, 2) \
            .when(F.col("min_temp_c") < 0, 1) \
            .otherwise(0)
        )
        
        df = df.withColumn(
            "precip_extreme_score",
            F.when(F.col("precipitation_mm") > 50, 3)
             .when(F.col("precipitation_mm") > 25, 2)
             .when(F.col("precipitation_mm") == 0, 1)
             .otherwise(0)
        )

        df = df.withColumn(
            "spi_extreme_score",
            F.when(F.abs(F.col("spi_index")) >= 2.0, 3) 
             .when(F.abs(F.col("spi_index")) >= 1.5, 2)
             .when(F.abs(F.col("spi_index")) >= 1.0, 1)
             .otherwise(0)
        )

        df = df.withColumn(
            "climate_risk_score",
            F.round(
                F.col("temp_extreme_score") * 0.5 +
                F.col("precip_extreme_score") * 0.3 +
                F.col("spi_extreme_score") * 0.2,
                2
            )
        )
        
        df = df.withColumn(
            "climate_risk_level",
            F.when(F.col("climate_risk_score") >= 2.5, "CRITICAL")
             .when(F.col("climate_risk_score") >= 1.5, "HIGH")
             .when(F.col("climate_risk_score") >= 0.5, "MODERATE")
             .otherwise("LOW")
        )

        df = df.drop("temp_extreme_score", "precip_extreme_score", "spi_extreme_score")
        return df
            

    def cleaning_data(self):
        super().cleaning_data()
        logger.info('Start remove invalid temperature relationships...')
        mn = F.col("min_temp_c")
        mx = F.col("max_temp_c")
        avg = F.col("avg_temp_c")
        self.df = self.df.filter(mn.isNull() | mx.isNull() | avg.isNull() | ((mn <= avg) & (avg <= mx)))
        

    def handle_missing_values(self):
        super().handle_missing_values()

        mean_cols = self.config.handle_mean_cols
        median_cols = self.config.handle_median_cols
        
        temp_columns = {"min_temp_c", "avg_temp_c", "max_temp_c"}
        
        temp_mean_cols = [c for c in (mean_cols or []) if c in temp_columns]
        temp_median_cols = [c for c in (median_cols or []) if c in temp_columns]
        
        logger.info("Starting temperature missing value imputation...")
        logger.info(f"Temperature columns for imputation: {temp_mean_cols + temp_median_cols}")
        
        if not (temp_mean_cols or temp_median_cols):
            logger.info("No temperature columns configured for imputation")
            return
        
        temp_aggs = []
        
        for c in temp_mean_cols:
            if c in self.df.columns:
                temp_aggs.append(F.avg(F.col(c).cast("double")).alias(f"__{c}_mean"))
        
        for c in temp_median_cols:
            if c in self.df.columns:
                temp_aggs.append(F.expr(f"percentile_approx({c}, 0.5)").alias(f"__{c}_median"))
        
        if not temp_aggs:
            logger.info("No valid temperature columns found in DataFrame")
            return
        
        temp_stats = self.df.groupBy("station_id").agg(*temp_aggs)
        df = self.df.join(temp_stats, on="station_id", how="left")
        
        min_temp = F.col("min_temp_c")
        avg_temp = F.col("avg_temp_c")
        max_temp = F.col("max_temp_c")
        
        imputation_values = {}
        
        for c in temp_mean_cols:
            if f"__{c}_mean" in df.columns:
                imputation_values[c] = F.col(f"__{c}_mean")
        
        for c in temp_median_cols:
            if f"__{c}_median" in df.columns:
                imputation_values[c] = F.col(f"__{c}_median")
        
        logger.info(f"Imputation values available for: {list(imputation_values.keys())}")
        
        if "avg_temp_c" in imputation_values:
            proposed_avg = imputation_values["avg_temp_c"]
            df = df.withColumn(
                "avg_temp_c",
                F.when(
                    avg_temp.isNull() & min_temp.isNotNull() & max_temp.isNotNull(),
                    F.when(
                        (proposed_avg >= min_temp) & (proposed_avg <= max_temp),
                        proposed_avg
                    ).otherwise((min_temp + max_temp) / 2)
                ).when(
                    avg_temp.isNull() & min_temp.isNotNull() & max_temp.isNull(),
                    F.greatest(proposed_avg, min_temp)
                ).when(
                    avg_temp.isNull() & min_temp.isNull() & max_temp.isNotNull(),
                    F.least(proposed_avg, max_temp)
                ).when(
                    avg_temp.isNull(),
                    proposed_avg
                ).otherwise(avg_temp)
            )
            logger.info("Applied avg_temp_c imputation with logic constraints")
        
        if "min_temp_c" in imputation_values:
            proposed_min = imputation_values["min_temp_c"]
            df = df.withColumn(
                "min_temp_c",
                F.when(
                    min_temp.isNull() & avg_temp.isNotNull() & max_temp.isNotNull(),
                    F.least(proposed_min, avg_temp)
                ).when(
                    min_temp.isNull() & avg_temp.isNotNull() & max_temp.isNull(),
                    F.least(proposed_min, avg_temp)
                ).when(
                    min_temp.isNull() & avg_temp.isNull() & max_temp.isNotNull(),
                    F.least(proposed_min, max_temp)
                ).when(
                    min_temp.isNull(),
                    proposed_min
                ).otherwise(min_temp)
            )
            logger.info("Applied min_temp_c imputation with logic constraints")
        
        if "max_temp_c" in imputation_values:
            proposed_max = imputation_values["max_temp_c"]
            df = df.withColumn(
                "max_temp_c",
                F.when(
                    max_temp.isNull() & avg_temp.isNotNull() & min_temp.isNotNull(),
                    F.greatest(proposed_max, avg_temp)
                ).when(
                    max_temp.isNull() & avg_temp.isNotNull() & min_temp.isNull(),
                    F.greatest(proposed_max, avg_temp)
                ).when(
                    max_temp.isNull() & avg_temp.isNull() & min_temp.isNotNull(),
                    F.greatest(proposed_max, min_temp)
                ).when(
                    max_temp.isNull(),
                    proposed_max
                ).otherwise(max_temp)
            )
            logger.info("Applied max_temp_c imputation with logic constraints")
        
        df = df.withColumn(
            "min_temp_c",
            F.when(
                F.col("min_temp_c").isNotNull() & F.col("avg_temp_c").isNotNull() & 
                (F.col("min_temp_c") > F.col("avg_temp_c")),
                F.col("avg_temp_c")
            ).otherwise(F.col("min_temp_c"))
        ).withColumn(
            "max_temp_c", 
            F.when(
                F.col("max_temp_c").isNotNull() & F.col("avg_temp_c").isNotNull() & 
                (F.col("max_temp_c") < F.col("avg_temp_c")),
                F.col("avg_temp_c")  
            ).otherwise(F.col("max_temp_c"))
        )
        
        temp_cols_to_drop = [col for col in df.columns if col.startswith("__") and any(temp in col for temp in ["min_temp_c", "avg_temp_c", "max_temp_c"])]
        if temp_cols_to_drop:
            df = df.drop(*temp_cols_to_drop)
            logger.info(f"Cleaned up temporary columns: {temp_cols_to_drop}")
        
        self.df = df
        
        logger.info("Temperature missing value imputation completed with logical constraints")
    
    def standardize_data(self):
        all_transforms = {
            "date": F.to_date(F.col("date"), "yyyy-MM-dd"),
            "city_name": F.initcap(F.trim(F.col("city_name"))),
            "season": F.initcap(F.trim(F.col("season")))
        }
        
        logger.info(f"Rounding double columns to 1 decimal place: {self.config.double_cols}")
        for col_name in self.config.double_cols:
            all_transforms[col_name] = F.round(F.col(col_name).cast("double"), 1)
            
        valid_transforms = [
            (col, expr) for col, expr in all_transforms.items() if col in self.df.columns
        ]
        
        self.df = functools.reduce(
            lambda df, item: df.withColumn(item[0], item[1]),
            valid_transforms, 
            self.df
        )
        logger.info("Weather data standardized")

    def enrich_data(self):
        def add_temp_range(df):
            if {"max_temp_c","min_temp_c"}.issubset(df.columns):
                mx = F.col("max_temp_c")
                mn = F.col("min_temp_c")
        
                return (
                    df
                    .withColumn(
                        "temp_range_c",
                        F.when(mx.isNull() | mn.isNull(), F.lit(None).cast("double"))
                         .otherwise(F.round(F.greatest(mx, mn) - F.least(mx, mn),1))
                    )
                )
            return df
    
        def add_temp_category(df):
            if "avg_temp_c" not in df.columns: return df
            temp = F.col("avg_temp_c").cast("double")
            return df.withColumn(
                "temp_category",
                F.when(temp.isNull(), None)
                 .when(temp < 0,  "Freezing")
                 .when(temp < 10, "Cold")
                 .when(temp < 20, "Mild")
                 .when(temp < 30, "Warm")
                 .otherwise("Hot")
            )
    
        def add_precip_category(df):
            if "precipitation_mm" not in df.columns: return df
            precipitation = F.col("precipitation_mm").cast("double")
            return df.withColumn(
                "precipitation_category",
                F.when(precipitation.isNull(), None)
                 .when(precipitation <= 0,  "No Rain")
                 .when(precipitation < 2.5, "Light Rain")
                 .when(precipitation < 10,  "Moderate Rain")
                 .when(precipitation < 50,  "Heavy Rain")
                 .otherwise("Very Heavy Rain")
            )
    
        self.df = (self.df
                   .transform(add_temp_range)
                   .transform(add_temp_category)
                   .transform(add_precip_category)
                   .transform(self._calculate_spi_index)
                   .transform(self._calculate_climate_risk_scoring)
                   )
        logger.info("Weather data enriched with additional context")

    def validate_data(self, referencial_table=None, max_invalid_ratio=0.02, min_rows=100, debug_mode=True):
        
        if self.df is None:
            logger.warning("DataFrame is not available; skipping validation.")
            return
        
        logger.info("="*60)
        logger.info("Starting Data Validation Pipeline")
        logger.info("="*60)
        
        df = self.df
        df.cache()
        initial_count = df.count()
        
        validation_report = {
            'initial_count': initial_count,
            'checks_performed': [],
            'issues_found': {},
            'metrics': {}
        }
        
        if debug_mode:
            logger.info(f"DEBUG: Initial DataFrame count: {initial_count}")
        

        logger.info("\n[1/4] SCHEMA VALIDATION")
        logger.info("-" * 40)
        
        required_columns = ['station_id', 'city_name', 'date', 'season', 
                          'avg_temp_c', 'min_temp_c', 'max_temp_c', 'precipitation_mm']
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Schema validation failed: Missing columns {missing_columns}")
        
        logger.info("Schema validation passed")
        validation_report['checks_performed'].append('Schema Validation')
        

        logger.info("\n[2/4] NULL VALUE CHECK")
        logger.info("-" * 40)
        
        null_counts = {}
        for col in df.columns:
            try:
                null_count = df.filter(F.col(col).isNull()).count()
                null_counts[col] = null_count
                completeness = ((initial_count - null_count) / initial_count * 100) if initial_count > 0 else 0
                logger.info(f"  {col}: {completeness:.1f}% complete ({null_count} nulls)")
            except Exception as e:
                logger.warning(f"Could not check nulls for {col}: {e}")
                null_counts[col] = 0
        
        for col in self.config.primary_key_cols:
            if null_counts.get(col, 0) > 0:
                logger.warning(f"Removing {null_counts[col]} rows with null {col}")
                df = df.filter(F.col(col).isNotNull())
        
        validation_report['metrics']['null_counts'] = null_counts
        validation_report['checks_performed'].append('Null Check')
        
        logger.info("\n[3/4] DEDUPLICATION")
        logger.info("-" * 40)
        
        try:
            pre_dedup_count = df.count()
            df = df.dropDuplicates(["station_id", "date"])
            post_dedup_count = df.count()
            duplicate_count = pre_dedup_count - post_dedup_count
            
            if duplicate_count > 0:
                logger.warning(f"Removed {duplicate_count} duplicate records")
                validation_report['issues_found']['duplicates'] = duplicate_count
            else:
                logger.info("No duplicates found")
                
        except Exception as e:
            logger.warning(f"Deduplication failed: {e}, continuing without deduplication")
        
        validation_report['checks_performed'].append('Deduplication')
        
        logger.info("\n[4/4] BASIC BUSINESS RULES")
        logger.info("-" * 40)
        rules_applied = 0
        
        try:
            temp_logic_filter = (
                (F.col("min_temp_c").isNull()) | 
                (F.col("avg_temp_c").isNull()) | 
                (F.col("max_temp_c").isNull()) | 
                (
                    (F.col("min_temp_c") <= F.col("avg_temp_c")) & 
                    (F.col("avg_temp_c") <= F.col("max_temp_c"))
                )
            )
            
            pre_temp_count = df.count()
            df = df.filter(temp_logic_filter)
            post_temp_count = df.count()
            temp_violations = pre_temp_count - post_temp_count
            
            if temp_violations > 0:
                logger.warning(f"  Removed {temp_violations} temperature logic violations")
            else:
                logger.info("Temperature logic check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Temperature logic check failed: {e}")
        
        try:
            temp_range_filter = (
                (F.col("avg_temp_c").isNull()) | 
                ((F.col("avg_temp_c") >= -60) & (F.col("avg_temp_c") <= 60))
            )
            
            pre_range_count = df.count()
            df = df.filter(temp_range_filter)
            post_range_count = df.count()
            range_violations = pre_range_count - post_range_count
            
            if range_violations > 0:
                logger.warning(f"  Removed {range_violations} extreme temperature outliers")
            else:
                logger.info("Temperature range check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Temperature range check failed: {e}")
        
        try:
            precip_filter = (
                (F.col("precipitation_mm").isNull()) | 
                (F.col("precipitation_mm") >= 0)
            )
            
            pre_precip_count = df.count()
            df = df.filter(precip_filter)
            post_precip_count = df.count()
            precip_violations = pre_precip_count - post_precip_count
            
            if precip_violations > 0:
                logger.warning(f"  Removed {precip_violations} negative precipitation values")
            else:
                logger.info("Precipitation check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Precipitation check failed: {e}")
        
        if referencial_table is not None:
            try:
                valid_stations = [row['station_id'] for row in referencial_table.select("station_id").distinct().collect()]
                
                pre_fk_count = df.count()
                df = df.filter(F.col("station_id").isin(valid_stations))
                post_fk_count = df.count()
                fk_violations = pre_fk_count - post_fk_count
                
                if fk_violations > 0:
                    logger.warning(f"  Removed {fk_violations} invalid station references")
                    validation_report['issues_found']['referential_integrity'] = fk_violations
                else:
                    logger.info("Referential integrity check passed")
                
                rules_applied += 1
                
            except Exception as e:
                logger.warning(f"Referential integrity check failed: {e}")
        
        logger.info(f"Applied {rules_applied} business rules successfully")
        validation_report['checks_performed'].append('Basic Business Rules')

        
        logger.info("\n" + "="*60)
        logger.info("QUALITY GATE CHECKS")
        logger.info("="*60)
        
        final_count = df.count()
        
        if final_count < min_rows:
            logger.error(f"FAILED: Only {final_count} final rows (minimum: {min_rows})")
            raise ValueError(f"Quality gate failed: Insufficient records after validation")
        else:
            logger.info(f"PASSED: {final_count} final rows (minimum: {min_rows})")
        
        drop_ratio = (initial_count - final_count) / initial_count if initial_count > 0 else 0
        if drop_ratio > max_invalid_ratio:
            logger.warning(f"WARNING: Drop ratio {drop_ratio:.2%} exceeds threshold {max_invalid_ratio:.2%}")
        else:
            logger.info(f"PASSED: Drop ratio {drop_ratio:.2%} within threshold {max_invalid_ratio:.2%}")
        
        logger.info(f"\n" + "="*60)
        logger.info(f"VALIDATION COMPLETE")
        logger.info(f"="*60)
        logger.info(f"Initial records:  {initial_count:,}")
        logger.info(f"Final records:    {final_count:,}")
        logger.info(f"Records dropped:  {initial_count - final_count:,}")
        logger.info(f"Success rate:     {final_count/initial_count*100:.2f}%" if initial_count > 0 else "N/A")
        
        df.cache()
        self.df.unpersist()
        self.df = df
        
        validation_report['final_count'] = final_count
        validation_report['drop_ratio'] = drop_ratio
        return validation_report