from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

from src.extract_data import extract_weather_data
from src.load_data import load_weather_data
from src.transform_data import data_transformations
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

API_KEY = os.getenv('API_KEY')
# url = f'https://api.openweathermap.org/data/2.5/weather?q=Sao Paulo,BR&units=metric&appid={API_KEY}'
url = os.getenv('url')
url = url + API_KEY

print(url)

@dag(
    dag_id='youtube_weather_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL - CLima SP',
    schedule='0 */1 * * * ',
    start_date=datetime(2026, 2, 7),
    catchup=False,
    tags=['weather', 'etl', 'se inscreve no canal!']
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
        # Read with Spark and load natively
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("AirflowLoad").getOrCreate()
        df = spark.read.parquet('/opt/airflow/data/temp_data.parquet')
        load_weather_data('sp_weather', df)

    extract() >> transform() >> load()


weather_pipeline()