#author NG CHIAO HAN
class Neo4jQuery:
    def __init__(self, utils_neo4j_instance):
        self.driver = utils_neo4j_instance.neo4j_driver

    def create_relationships(self):
        with self.driver.session() as session:
            session.run("""
                MATCH (city:City), (country:Country)
                WHERE city.iso3 = country.iso3
                MERGE (city)-[:LOCATED_IN {source:'cities.csv'}]->(country)
            """)
            print("Linked City -> Country")

            session.run("""
                MATCH (country:Country), (co2:Co2)
                WHERE country.country = co2.country
                MERGE (country)-[:HAS_CO2]->(co2)
                  ON CREATE SET co2.emissions = co2.MtCO2_per_day,
                                co2.date = co2.date
                  ON MATCH SET co2.emissions = co2.MtCO2_per_day
            """)
            print("Linked Country -> Co2")

            session.run("""
                MATCH (city:City), (w:Weather)
                WHERE city.station_id = w.station_id
                MERGE (city)-[:HAS_WEATHER]->(w)
                  ON CREATE SET w.season = w.season
                  ON MATCH SET w.season = w.season
            """)
            print("Linked City -> Weather")

            session.run("""
                MATCH (w:Weather)
                MERGE (d:Date {value: w.date})
                MERGE (w)-[:ON_DATE]->(d)
            """)
            print("Created Weather -> ON_DATE -> Date")

            session.run("""
                MATCH (co2:Co2)
                WHERE co2.sector IS NOT NULL
                MERGE (s:Sector {name: co2.sector})
                MERGE (co2)-[:EMIT_FROM]->(s)
            """)
            print("Linked CO2 -> Sector (EMIT_FROM)")

            session.run("""
                MATCH (co2:Co2)
                MERGE (e:EmissionValue {date: co2.date, country: co2.country, sector: co2.sector})
                  ON CREATE SET e.mtCo2PerDay = co2.MtCO2_per_day
                  ON MATCH SET e.mtCo2PerDay = co2.MtCO2_per_day
                MERGE (co2)-[:HAS_VALUE]->(e)
            """)
            print("Linked CO2 -> EmissionValue (HAS_VALUE)")

    def get_monthly_temp_and_co2(self, limit=30):
        query = """
            MATCH (c:Country)<-[:LOCATED_IN]-(city:City)-[:HAS_WEATHER]->(w:Weather),
                  (c)-[:HAS_CO2]->(co:Co2)
            WHERE w.date = co.date
            WITH c.country AS country,
                 w.date AS date,
                 avg(w.avg_temp_c) AS daily_avg_temp,
                 sum(co.MtCO2_per_day) AS daily_total_co2,
                 substring(w.date,0,7) AS month
            WITH country, date, daily_avg_temp, daily_total_co2, month
            ORDER BY country, date
            WITH country, 
                 month,
                 collect({date: date, avg_temp: daily_avg_temp, co2: daily_total_co2})[0..5] AS daily_sample,
                 avg(daily_avg_temp) AS monthly_avg_temp,
                 sum(daily_total_co2) AS monthly_total_co2
            RETURN country,
                   month,
                   monthly_avg_temp,
                   monthly_total_co2,
                   daily_sample
            ORDER BY country, month
            LIMIT $limit
        """
    
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return list(result)

    def get_hot_cities_vs_co2(self, limit=20):
        query = """
            MATCH (city:City)-[:HAS_WEATHER]->(w:Weather)
            WHERE w.avg_temp_c > 30
            WITH city, collect(w.date) AS hot_days
            WITH city, size(hot_days) AS hot_days_count
            MATCH (city)-[:LOCATED_IN]->(country:Country)-[:HAS_CO2]->(co:Co2)
            RETURN city.city_name AS city,
                   country.country AS country,
                   hot_days_count,
                   avg(co.MtCO2_per_day) AS avg_co2
            ORDER BY hot_days_count DESC
            LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [record.data() for record in result] 

    def close(self):
        if self.driver:
            self.driver.close()
            print("Neo4j driver closed.")

