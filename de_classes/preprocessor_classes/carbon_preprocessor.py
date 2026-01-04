#author THAM ZHEN HERN
from .preprocessor import Preprocessor
from .data_preprocessing_config import DataPreprocessingConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CarbonPreprocessor(Preprocessor):   
    def __init__(self, spark:SparkSession,input_path: str, output_path: str, config: DataPreprocessingConfig):
        super().__init__(spark,input_path, output_path, config)
        self.__name__ = "CarbonPreprocessor"

    def handle_missing_values(self):
        super().handle_missing_values()
        if self.config.handle_median_cols:
            logger.info(f"Calculating approximate median for columns: {self.config.handle_median_cols}")
            medians = self.df.select([F.expr(f'percentile_approx({c}, 0.5)').alias(c) for c in self.config.handle_median_cols]).collect()[0].asDict()
            self.df = self.df.na.fill(medians)
            
    def standardize_data(self):
        if "date" in self.df.columns:
            raw = F.col("date").cast("string")
            
            formatted_8digit = F.when(
                raw.rlike(r"^\d{8}$"),
                F.concat_ws("-", 
                           raw.substr(5, 4),
                           raw.substr(3, 2),
                           raw.substr(1, 2))
            )
            
            parsed = F.coalesce(
                F.to_date(raw, "dd/MM/yyyy"),
                F.to_date(raw, "d/M/yyyy"),    
                F.to_date(raw, "yyyy-MM-dd"),
                F.to_date(raw, "yyyy/MM/dd"),
                F.to_date(formatted_8digit, "yyyy-MM-dd")
            )
            
            self.df = self.df.withColumn("date", parsed)
        
        transformations = {
            "country": F.initcap(F.trim(F.col("country"))),
            "sector": F.initcap(F.trim(F.col("sector")))   
        }
    
        df_to_transform = self.df
        for col_name, expr in transformations.items():
            if col_name in df_to_transform.columns:
                df_to_transform = df_to_transform.withColumn(col_name, expr)
    
        logger.info(f"Rounding double columns to 6 decimal place: {self.config.double_cols}")
        for col_name in self.config.double_cols:
            if col_name in df_to_transform.columns:
                df_to_transform = df_to_transform.withColumn(col_name, F.round(F.col(col_name), 6))
        
        self.df = df_to_transform
        logger.info("Carbon data standardized")
    
    
    def enrich_data(self):
        if 'MtCO2_per_day' in self.df.columns:
            self.df = self.df.withColumn(
                "emission_level",
                F.when(F.col("MtCO2_per_day") == 0, "Zero")
                .when(F.col("MtCO2_per_day") < 0.1, "Very Low")
                .when(F.col("MtCO2_per_day") < 1, "Low")
                .when(F.col("MtCO2_per_day") < 10, "Medium")
                .when(F.col("MtCO2_per_day") < 50, "High")
                .otherwise("Very High")
            )
        
        if 'sector' in self.df.columns:
            self.df = self.df.withColumn(
                "sector_category",
                F.when(F.col("sector").isin(["power", "electricity", "energy"]), "Energy")
                .when(F.col("sector").isin(["transport", "aviation", "shipping"]), "Transportation")
                .when(F.col("sector").isin(["industry", "manufacturing"]), "Industrial")
                .when(F.col("sector").isin(["residential", "commercial"]), "Buildings")
                .otherwise("Other")
            )
        
        logger.info("Carbon data enriched with additional context")
    
    def validate_data(self, referencial_table=None, max_invalid_ratio=0.05, min_rows=50, debug_mode=True):
        if self.df is None:
            logger.warning("DataFrame is not available; skipping validation.")
            return
        
        logger.info("="*60)
        logger.info("Starting Carbon Data Validation Pipeline")
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
        
        logger.info("\n[1/5] SCHEMA VALIDATION")
        logger.info("-" * 40)
        
        required_columns = ['country', 'sector', 'date', 'MtCO2_per_day']
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Schema validation failed: Missing columns {missing_columns}")
        
        logger.info("Schema validation passed")
        validation_report['checks_performed'].append('Schema Validation')

        logger.info("\n[2/5] NULL VALUE CHECK")
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
        
        logger.info("\n[3/5] DEDUPLICATION")
        logger.info("-" * 40)
        
        try:
            pre_dedup_count = df.count()
            df = df.dropDuplicates(["country", "sector", "date"])
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
        
        logger.info("\n[4/5] EMISSION DATA RULES")
        logger.info("-" * 40)
        rules_applied = 0
        
        try:
            negative_filter = (
                (F.col("MtCO2_per_day").isNull()) | 
                (F.col("MtCO2_per_day") >= 0)
            )
            
            pre_negative_count = df.count()
            negative_emissions = df.filter(F.col("MtCO2_per_day") < 0).count()
            df = df.filter(negative_filter)
            post_negative_count = df.count()
            
            if negative_emissions > 0:
                logger.warning(f"  Removed {negative_emissions} records with negative emissions")
                validation_report['issues_found']['negative_emissions'] = negative_emissions
            else:
                logger.info("Negative emission check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Negative emission check failed: {e}")
        
        try:
            unrealistic_filter = (
                (F.col("MtCO2_per_day").isNull()) | 
                (F.col("MtCO2_per_day") <= 1000)
            )
            
            pre_unrealistic_count = df.count()
            unrealistic_emissions = df.filter(F.col("MtCO2_per_day") > 1000).count()
            df = df.filter(unrealistic_filter)
            post_unrealistic_count = df.count()
            
            if unrealistic_emissions > 0:
                logger.warning(f"  Removed {unrealistic_emissions} records with unrealistic emissions")
                validation_report['issues_found']['unrealistic_emissions'] = unrealistic_emissions
            else:
                logger.info("Unrealistic emission check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Unrealistic emission check failed: {e}")
        
        try:
            date_filter = (
                (F.col("date").isNull()) |
                ((F.col("date") >= "2019-01-01") & (F.col("date") <= F.current_date()))
            )
            
            pre_date_count = df.count()
            df = df.filter(date_filter)
            post_date_count = df.count()
            date_violations = pre_date_count - post_date_count
            
            if date_violations > 0:
                logger.warning(f"  Removed {date_violations} records with invalid dates")
                validation_report['issues_found']['invalid_dates'] = date_violations
            else:
                logger.info("Date range check passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Date range check failed: {e}")
        
        try:
            country_sector_filter = (
                (F.col("country").isNotNull()) & 
                (F.length(F.col("country")) > 0) &
                (F.col("sector").isNotNull()) & 
                (F.length(F.col("sector")) > 0)
            )
            
            pre_cs_count = df.count()
            df = df.filter(country_sector_filter)
            post_cs_count = df.count()
            cs_violations = pre_cs_count - post_cs_count
            
            if cs_violations > 0:
                logger.warning(f"  Removed {cs_violations} records with empty country/sector")
                validation_report['issues_found']['empty_country_sector'] = cs_violations
            else:
                logger.info("Country/Sector validation passed")
            
            rules_applied += 1
            
        except Exception as e:
            logger.warning(f"Country/Sector validation failed: {e}")
        
        if referencial_table is not None:
            try:
                if hasattr(referencial_table, 'select'):  
                    valid_countries = [row['country'] for row in referencial_table.select("country").distinct().collect()]
                else:
                    valid_countries = referencial_table
                
                pre_ref_count = df.count()
                df = df.filter(F.col("country").isin(valid_countries))
                post_ref_count = df.count()
                ref_violations = pre_ref_count - post_ref_count
                
                if ref_violations > 0:
                    logger.warning(f"  Removed {ref_violations} records with invalid country references")
                    validation_report['issues_found']['referential_integrity'] = ref_violations
                else:
                    logger.info("Country referential integrity check passed")
                
                rules_applied += 1
                
            except Exception as e:
                logger.warning(f"Country referential integrity check failed: {e}")
        
        logger.info(f"Applied {rules_applied} business rules successfully")
        validation_report['checks_performed'].append('Emission Business Rules')
        

        logger.info("\n[5/5] DATA QUALITY METRICS")
        logger.info("-" * 40)
        
        try:
            country_count = df.select("country").distinct().count()
            sector_count = df.select("sector").distinct().count()
            date_range = df.select(F.min("date").alias("min_date"), F.max("date").alias("max_date")).collect()[0]
            
            emission_stats = df.select(
                F.min("MtCO2_per_day").alias("min_emission"),
                F.max("MtCO2_per_day").alias("max_emission"),
                F.avg("MtCO2_per_day").alias("avg_emission"),
                F.count("MtCO2_per_day").alias("emission_count")
            ).collect()[0]
            
            logger.info(f"  Countries: {country_count}")
            logger.info(f"  Sectors: {sector_count}")
            logger.info(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")
            logger.info(f"  Emission range: {emission_stats['min_emission']:.6f} to {emission_stats['max_emission']:.6f} MtCO2/day")
            logger.info(f"  Average emission: {emission_stats['avg_emission']:.6f} MtCO2/day")
            
            validation_report['metrics']['countries'] = country_count
            validation_report['metrics']['sectors'] = sector_count
            validation_report['metrics']['date_range'] = {
                'min_date': str(date_range['min_date']),
                'max_date': str(date_range['max_date'])
            }
            validation_report['metrics']['emission_stats'] = {
                'min': float(emission_stats['min_emission']),
                'max': float(emission_stats['max_emission']),
                'avg': float(emission_stats['avg_emission']),
                'count': emission_stats['emission_count']
            }
            
        except Exception as e:
            logger.warning(f"Data quality metrics calculation failed: {e}")
        
        validation_report['checks_performed'].append('Data Quality Metrics')
        
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
        
        try:
            if 'countries' in validation_report['metrics'] and validation_report['metrics']['countries'] < 5:
                logger.warning(f"WARNING: Only {validation_report['metrics']['countries']} countries in dataset")
            
            if 'sectors' in validation_report['metrics'] and validation_report['metrics']['sectors'] < 3:
                logger.warning(f"WARNING: Only {validation_report['metrics']['sectors']} sectors in dataset")
                
        except Exception as e:
            logger.warning(f"Coverage check failed: {e}")
        
        logger.info(f"\n" + "="*60)
        logger.info(f"CARBON DATA VALIDATION COMPLETE")
        logger.info(f"="*60)
        logger.info(f"Initial records:     {initial_count:,}")
        logger.info(f"Final records:       {final_count:,}")
        logger.info(f"Records dropped:     {initial_count - final_count:,}")
        logger.info(f"Success rate:        {final_count/initial_count*100:.2f}%" if initial_count > 0 else "N/A")
        logger.info(f"Checks performed:    {len(validation_report['checks_performed'])}")
        logger.info(f"Issues found:        {len(validation_report['issues_found'])}")
        
        df.cache()
        self.df.unpersist()
        self.df = df
        
        validation_report['final_count'] = final_count
        validation_report['drop_ratio'] = drop_ratio
        
        return validation_report