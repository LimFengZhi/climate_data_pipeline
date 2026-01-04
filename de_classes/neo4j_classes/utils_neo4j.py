#author NG CHIAO HAN
import os
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, StringType
from pyspark.sql import functions as F
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ClientError, ServiceUnavailable

class UtilsNeo4j:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, redis_client=None):
        self.neo4j_driver = self._setup_neo4j_driver(neo4j_uri, neo4j_user, neo4j_password)
        self.redis_client = redis_client

    def _setup_neo4j_driver(self, uri, user, password):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            print("Successfully connected to Neo4j.")
            return driver
        except ServiceUnavailable as e:
            print(f"Failed to connect to Neo4j: {e}")
            raise

    def _read_df(self, spark, path: str) -> DataFrame:
        p = (path or "").lower()
        if p.endswith(".parquet") or p.endswith("/"):
            return spark.read.parquet(path)
        if p.endswith(".csv"):
            return spark.read.csv(path, header=True, inferSchema=True)

        try:
            return spark.read.parquet(path)
        except Exception:
            return spark.read.csv(path, header=True, inferSchema=True)

    def _cast_dates_to_string(self, df: DataFrame) -> DataFrame:
        for field in df.schema.fields:
            if isinstance(field.dataType, DateType):
                df = df.withColumn(field.name, F.col(field.name).cast(StringType()))
            elif isinstance(field.dataType, StringType) and field.name.lower() in {"date"}:
                df = df.withColumn(field.name, F.to_date(F.col(field.name), "yyyy-MM-dd").cast(StringType()))
        return df

    def _create_node_query(self, label: str, properties: dict) -> tuple[str, dict]:
        props_str = ", ".join(f"{k}: ${k}" for k in properties.keys())
        query = f"MERGE (n:{label} {{{props_str}}}) RETURN n"
        return query, properties
    
    def _insert_df(self, df: DataFrame, label: str, batch_size: int = 1000) -> int:
        total_inserted = 0
        batch = []
        
        rows = df.collect()

        with self.neo4j_driver.session() as session:
            print(f"[OVERWRITE] Deleting all nodes with label '{label}'...")
            session.run(f"MATCH (n:{label}) DETACH DELETE n")
            print(f"[OVERWRITE] Deleted nodes with label '{label}'.")

            for row in rows:
                row_dict = row.asDict(recursive=True)
                query, params = self._create_node_query(label, row_dict)
                batch.append((query, params))

                if len(batch) >= batch_size:
                    try:
                        session.write_transaction(lambda tx, queries: [tx.run(q, p) for q, p in queries], batch)
                        total_inserted += len(batch)
                        print(f"[INSERT] {total_inserted} nodes -> ':{label}'")
                        batch.clear()
                    except (ClientError, ServiceUnavailable) as e:
                        print(f"[ERROR] Batch write failed: {e}")
                        batch.clear()
            
            if batch:
                try:
                    session.write_transaction(lambda tx, queries: [tx.run(q, p) for q, p in queries], batch)
                    total_inserted += len(batch)
                    print(f"[INSERT] {total_inserted} nodes -> ':{label}' (final)")
                except (ClientError, ServiceUnavailable) as e:
                    print(f"[ERROR] Final batch write failed: {e}")

        return total_inserted

    def ingest_data(self, spark, path: str, label: str, batch_size: int = 1000) -> int:
        if not path:
            print(f"[SKIP] ':{label}': no path set in .env")
            return 0

        df = self._read_df(spark, path)
        df = self._cast_dates_to_string(df)

        print(f"[LOAD] ':{label}': {df.count()} rows from {path}")
        return self._insert_df(df, label, batch_size=batch_size)
    
    def ingest_all_from_list(self, spark, ingestion_list: list[tuple[str, str]], batch_size: int = 1000) -> None:
        for label, path in ingestion_list:
            print(f"\n[START] Ingesting {label} data from {path}")
            inserted_count = self.ingest_data(spark, path, label, batch_size=batch_size)
            print(f"[DONE] Ingested {inserted_count} nodes for label ':{label}'")