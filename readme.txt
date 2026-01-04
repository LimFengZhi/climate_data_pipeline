===========================================================
BMDS2013 Data Engineering Assignment – readme.txt
===========================================================


Team Reference: S1G2S-1
Submission Date: 29 August 2025


-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Real Time Analysis on 
Extreme Weather due to Emission of Carbon dioxide Monitor 
(SDG #13 – Climate Action)
-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
/climate_data_pipeline/
│
├── Task2_Data_Preprocessing_Demo.ipynb
├── Task3_MongoDB_Demo.ipynb
├── Task4_Neo4j_Demo.ipynb
├── Task5_Spark_Structure_Streaming_Demo.ipynb
│
├── de_classes/
│   ├── consumer_classes/
│   │   ├── carbon_consumer.py
│   │   ├── consumer_config.py
│   │   ├── consumer.py
│   │   └── weather_consumer.py
│   │
│   ├── mongo_classes/
│   │   ├── mongo_query.py
│   │   └── utils_mongo.py
│   │
│   ├── neo4j_classes/
│   │   ├── neo4j_query.py
│   │   └── utils_neo4j.py
│   │
│   ├── preprocessor_classes/
│   │   ├── carbon_preprocessor.py
│   │   ├── data_preprocessing_config.py
│   │   ├── preprocessor.py
│   │   └── weather_preprocessor.py
│   │
│   ├── producer_classes/
│   │   └── producer.py
│   │
│   ├── streamer_classes/
│   │   ├── climate_risk_scoring_streamer.py
│   │   └── streamer.py
│   │
│   ├── utility_classes/
│   │   ├── input_country_manager.py
│   │   └── spark_manager.py
│
├── de_data/
│   ├── data_store/
│   │   ├── meta_data/
│   │   │   ├── de_cities.csv
│   │   │   └── de_countries.csv
│   │   │
│   │   ├── processed_data/
│   │   │   ├── cleanned_data/
│   │   │   │   ├── cleaned_co2_parquet/
│   │   │   │   │   ├── _SUCCESS
│   │   │   │   │   ├── part-00000-...parquet
│   │   │   │   │   └── ... (other parquet parts)
│   │   │   │   │
│   │   │   │   ├── cleaned_weather_parquet/
│   │   │   │       ├── _SUCCESS
│   │   │   │       ├── part-00000-...parquet
│   │   │   │       └── ... (other parquet parts)
│   │   │   │
│   │   │   ├── streamed_raw_data/
│   │   │       ├── raw_streamed_co2_parquet/
│   │   │       │   ├── _SUCCESS
│   │   │       │   ├── part-00000-...snappy.parquet
│   │   │       │   └── ... (other parquet parts)
│   │   │       │
│   │   │       ├── raw_streamed_weather_parquet/
│   │   │           ├── _SUCCESS
│   │   │           ├── part-00000-...snappy.parquet
│   │   │           └── ... (other parquet parts)
│   │
│   ├── raw_data/
│       ├── de_co2.csv
│       └── de_weather.csv
│
├── guideline.txt
├── requirements.txt
├── .env
├── run_carbon_consumer.py
├── run_carbon_producer.py
├── run_weather_consumer.py
└── run_weather_producer.py
    
-----------------------------------------------------------
3. Setup Instructions:
-----------------------------------------------------------
1. Make sure in project directory
    $ cd climate_data_pipeline

2. Create Python Environment 
   $ python -m venv pipelineVenv


3. Activate the environment
   $ source pipelineVenv/bin/activate


4. Install dependencies:
   $ pip install -r requirements.txt

   $ python -m ipykernel install --user --name=pipelineVenv --display-name "Pipeline Env"


5. Put the data file into HDFS
   $ hdfs dfs -put de_data/data_store data_store
   $ hdfs dfs -ls


6. Start Kafka server and necessary services (HDFS, YARN, Zookeeper, Kafka).


7. First, make sure the .env file is in the project folder.
      $ ls .env
      $ cat .env

8. To run the demo:

   Task 1
   —-----
    Use 4 terminal to run
   a. Run the Kafka Producer (use first 2 terminal)
      $ python run_carbon_producer.py
      $ python run_weather_producer.py


   b. Run the Kafka Consumer (use second 2 terminal)
      $ python run_carbon_consumer.py
      $ python run_weather_consumer.py


   Task 2
   —-----
        Open the Task2_Data_Preprocessing_Demo.ipynb in the Jupyter lab launched in your WSL Ubuntu instance.


   Task 3
   —-----
        Open the Task3_MongoDB_Demo.ipynb in the Jupyter lab launched in your WSL Ubuntu instance.
  
   Task 4
   —-----
        Open the Task4_Neo4j_Demo.ipynb in the Jupyter lab launched in your WSL Ubuntu instance.
  
   Task 5
   —-----
        Open the Task5_Spark_Structure_Streaming.ipynb in the Jupyter lab launched in your WSL Ubuntu instance.