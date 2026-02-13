import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from src import WeatherExtractor
from src import WeatherPostgresLoader
from src import WeatherTransformer
from src.interfaces import DataTransformer, DataExtractor, DataLoader
from src.transform_data import data_transformations

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class WeatherPipeline:
    """
    Orchestrates the weather ETL process using injected components
    With principle of IOC =  Inversion of control
    """

    def __init__(self,
                 extractor: DataExtractor,
                 transformer: DataTransformer,
                 loader: DataLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self, url: str, table_name: str):
        """Execute the full ETL pipeline"""
        try:
            logging.info(f"Starting ETL pipeline for {table_name}")

            logging.info("Step1: Extracting")
            raw_data = self.extractor.extract(url)

            logging.info("Step2: Transforming")
            transformed_df = data_transformations()

            logging.info("Step3: Loading")
            self.loader.load(table_name, transformed_df)

            logging.info("----- Pipeline Completed Successfully -----")

        except Exception as e:
            logging.error(f"Pipeline Failed: {e}")
            raise

def main():
    env_path = Path(__file__).parent / "config" /".env"
    load_dotenv(env_path)

    api_key = os.getenv("API_KEY") or os.getenv("api_key")
    if not api_key:
        logging.error("API_KEY not set")
        return

    url = os.getenv('url')
    url = url + api_key
    table_name = "sp_weather"

    if not url:
        raise ValueError("url not found in .env file")

    extractor = WeatherExtractor()
    transformer = WeatherTransformer()
    loader = WeatherPostgresLoader()

    pipeline = WeatherPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader
    )

    pipeline.run(url, table_name)

if __name__ == "__main__":
    main()