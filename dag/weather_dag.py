from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys
import os

sys.path.insert(0, '/opt/airflow')

from src.extract_data import extract_weather_data
from src.transform_data import data_transformations
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

api_key = os.getenv('API_KEY') or os.getenv('api_key')
url = os.getenv('url') or os.getenv('URL')

if not api_key or not url:
    raise ValueError(f"API_KEY or url not found. API_KEY: {'Found' if api_key else 'Miss'}, url: {'Found' if url else 'Miss'}")

url = url + api_key

print(url)

@dag(
    dag_id='weather_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL - Weather SP',
    schedule='0 */1 * * * ',
    start_date=datetime(2026, 2, 7),
    catchup=False,
    tags=['weather', 'etl', 'python']
)
def weather_pipeline():
    @task
    def extract():
        extract_weather_data(url)

    @task
    def transform():
        df = data_transformations()
        # Save as Parquet natively with Spark
        df.write.mode("overwrite").parquet('/opt/airflow/data/temp_data.parquet')

    @task
    def load():
        db_host = os.getenv('POSTGRES_HOST') or 'postgres'
        
        from src.load_data import WeatherPostgresLoader
        loader = WeatherPostgresLoader(host=db_host)
        
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("WeatherETL-Load") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
            .getOrCreate()
        df = spark.read.parquet('/opt/airflow/data/temp_data.parquet')
        loader.load('sp_weather', df)

    extract() >> transform() >> load()


weather_pipeline()