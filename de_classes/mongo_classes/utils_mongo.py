#author THO HUI YEE
import os
from typing import Optional
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pyspark.sql import DataFrame


class UtilsMongo:
    def __init__(self, db, spark):
        self.db = db
        self.spark = spark

    def read_df(self, path: str) -> DataFrame:
        p = (path or "").lower()
        if p.endswith(".parquet") or p.endswith("/"):
            return self.spark.read.parquet(path)
        if p.endswith(".csv"):
            return self.spark.read.csv(path, header=True, inferSchema=True)
        try:
            return self.spark.read.parquet(path)
        except Exception:
            return self.spark.read.csv(path, header=True, inferSchema=True)
    
    def insert_batches(
        self,
        coll: Collection,
        df: DataFrame,
        *,
        batch_size: int = 1000,
        overwrite: bool = False
    ) -> int:
        if overwrite:
            coll.delete_many({})
            print(f"[OVERWRITE] Cleared '{coll.name}'")
    
        def fix_types(row_dict: dict) -> dict:
            for k, v in row_dict.items():
                if hasattr(v, "isoformat"):
                    row_dict[k] = v.isoformat()
    
            if coll.name == "weathers":
                row_dict["temperature"] = {
                    "avg": row_dict.pop("avg_temp_c", None),
                    "min": row_dict.pop("min_temp_c", None),
                    "max": row_dict.pop("max_temp_c", None),
                    "range": row_dict.pop("temp_range_c", None),
                    "category": row_dict.pop("temp_category", None),
                }
                row_dict["precipitation"] = {
                    "value": row_dict.pop("precipitation_mm", None),
                    "category": row_dict.pop("precipitation_category", None),
                }
                row_dict["risk"] = {
                    "score": row_dict.pop("climate_risk_score", None),
                    "level": row_dict.pop("climate_risk_level", None),
                }
    
            elif coll.name == "carbons":
                row_dict["sector"] = {
                    "type": row_dict.pop("sector", None),
                    "category": row_dict.pop("sector_category", None),
                }
                if "MtCO2_per_day" in row_dict:
                    row_dict["mt_co2_per_day"] = row_dict.pop("MtCO2_per_day")
    
            elif coll.name == "countries":
                row_dict["capital"] = {
                    "name": row_dict.pop("capital", None),
                    "lat": row_dict.pop("capital_lat", None),
                    "long": row_dict.pop("capital_lng", None),
                }
    
            elif coll.name == "cities":
                row_dict["coordinates"] = {
                    "lat": row_dict.pop("latitude", None),
                    "long": row_dict.pop("longitude", None),
                }
    
            return row_dict
    
        total, batch = 0, []
        for row in df.toLocalIterator():
            batch.append(fix_types(row.asDict(recursive=True)))
            if len(batch) >= batch_size:
                try:
                    coll.insert_many(batch, ordered=False)
                except BulkWriteError:
                    pass
                total += len(batch)
                print(f"[INSERT] {total} docs -> '{coll.name}'")
                batch.clear()
    
        if batch:
            try:
                coll.insert_many(batch, ordered=False)
            except BulkWriteError:
                pass
            total += len(batch)
            print(f"[INSERT] {total} docs -> '{coll.name}' (final)")
    
        return total
    
    def ensure_collections(self):
        for name in ["countries", "cities", "weathers", "carbons"]:
            if name not in self.db.list_collection_names():
                self.db.create_collection(name)
                print(f"[CREATE] '{name}'")
    
        self.db.countries.create_index([("country", ASCENDING)], name="idx_country")
        self.db.cities.create_index([("city_name", ASCENDING)], name="idx_cityname")
        self.db.cities.create_index([("station_id", ASCENDING)], name="idx_station")
        self.db.weathers.create_index([("date", ASCENDING)], name="idx_weather_date")
        self.db.carbons.create_index([("date", ASCENDING)], name="idx_co2_date")
    
    def load_and_insert(
        self,
        path: Optional[str],
        coll_name: str,
        *,
        overwrite=True,
        batch_size=1000
    ):
        if not path:
            print(f"[SKIP] {coll_name}: no path provided")
            return 0
        df = self.read_df(path)
        return self.insert_batches(
            self.db[coll_name], df, batch_size=batch_size, overwrite=overwrite
        )
    
    def load_all(self, *, overwrite=True, batch_size=1000):
        self.ensure_collections()
        paths = {
            "countries": os.getenv("COUNTRY_CSV"),
            "cities": os.getenv("CITY_CSV"),
            "weathers": os.getenv("CLEANED_WEATHER"),
            "carbons": os.getenv("CLEANED_CO2"),
        }
        print("[PATHS]", paths)
    
        for coll, path in paths.items():
            print(f"\n[LOAD] {coll}")
            self.load_and_insert(
                path, coll, overwrite=overwrite, batch_size=batch_size
            )
    
    def list_countries(self):
        return [
            doc["country"]
            for doc in self.db.countries.find({}, {"_id": 0, "country": 1}).sort("country", 1)
        ]