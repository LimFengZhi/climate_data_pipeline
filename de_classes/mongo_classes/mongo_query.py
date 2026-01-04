#author THO HUI YEE
class MongoQuery:
    def __init__(self, db):
        self.db = db
        
    def list_countries(self):
        return [
            doc["country"]
            for doc in self.db.countries.find({}, {"_id": 0, "country": 1}).sort("country", 1)
        ]
        
    def query_top_emission_day_with_cities(self, country: str = None):
        match_stage = {}
        if country:
            match_stage["country"] = country
    
        pipeline = [
            {"$match": match_stage},
    
            {"$group": {
                "_id": {"date": "$date", "country": "$country"},
                "total_emission": {"$sum": "$mt_co2_per_day"}
            }},
            {"$sort": {"total_emission": -1}},
            {"$limit": 1},
    
            {"$lookup": {
                "from": "weathers",
                "let": {"d": "$_id.date", "c": "$_id.country"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$date", "$$d"]}}},
                    {"$lookup": {
                        "from": "cities",
                        "localField": "station_id",
                        "foreignField": "station_id",
                        "as": "city_info"
                    }},
                    {"$unwind": "$city_info"},
                    {"$match": {"$expr": {"$eq": ["$city_info.country", "$$c"]}}},
                    {"$group": {
                        "_id": "$city_info.city_name",
                        "max_temp": {"$max": "$temperature.max"}
                    }},
                    {"$sort": {"max_temp": -1}},
                    {"$limit": 5}  
                ],
                "as": "hottest_cities"
            }},
    
            {"$project": {
                "_id": 0,
                "date": "$_id.date",
                "country": "$_id.country",
                "total_emission": 1,
                "hottest_cities": 1
            }}
        ]
    
        return list(self.carbons.aggregate(pipeline))

    def query_emission_vs_extreme_weather(self, temp_threshold=35.0, risk_levels=None, limit=50):
        if risk_levels is None:
            risk_levels = ["HIGH", "SEVERE"]
    
        pipeline = [
            {"$match": {
                "$or": [
                    {"risk.level": {"$in": risk_levels}},
                    {"temperature.max": {"$gte": temp_threshold}}
                ]
            }},
    
            {"$lookup": {
                "from": "cities",
                "localField": "station_id",
                "foreignField": "station_id",
                "as": "city_info"
            }},
            {"$unwind": "$city_info"},
            
            {"$lookup": {
                "from": "carbons",
                "let": {"d": "$date", "c": "$city_info.country"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$date", "$$d"]},
                                {"$eq": ["$country", "$$c"]}
                            ]
                        }
                    }},
                    {"$group": {
                        "_id": None,
                        "total_emission": {"$sum": "$mt_co2_per_day"}
                    }},
                    {"$project": {
                        "_id": 0,
                        "total_emission": 1
                    }}
                ],
                "as": "emission_info"
            }},
            {"$unwind": "$emission_info"},
            
            {"$group": {
                "_id": {"country": "$city_info.country", "date": "$date"},
                "extreme_events": {"$sum": 1},
                "max_temp": {"$max": "$temperature.max"},
                "avg_risk": {"$avg": "$risk.score"},
                "total_emission": {"$first": "$emission_info.total_emission"}
            }},
           
            {"$match": {"max_temp": {"$gt": temp_threshold}}},
            {"$sort": {"total_emission": -1}},
            {"$limit": limit}
        ]
    
        return self.db.weathers.aggregate(pipeline)


