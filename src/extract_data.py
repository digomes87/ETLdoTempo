import requests
from fastapi import status
import json
from pathlib import Path
import logging
from typing import Any, Optional

from pyspark.core import status

from src.interfaces.etl_interfaces import DataExtractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class WeatherExtractor(DataExtractor):
    """
    Handles extraction from api weather
    Implements interface data extractor
    """

    def __init__(self, output_path: Optional[str]= None):
        self.output_path = output_path or 'data/weather_data.json'

    def extract(self, url: str) -> Any:
        """
        Extracts weather data from the given URL and saves it to a file
        """
        logging.info(f"Extracting weather data from {url}")

        try:
            response = requests.get(url)

            if response.status_code != status.HTTP_200_OK:
                logging.error(f"Request error: {response.status_code}")
                return []

            data = response.json()

            if not data:
                logging.warning("No data returned from API")
                return []

            self._save_to_json(data)
            return data

        except Exception as e:
            logging.error(f"Files to extract data: {e}")
            raise

    def _save_to_json(self, data: Any) -> None:
        """Helper to persist the raw data"""
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, 'w')as f:
            json.dump(data, f, indent=4)

        logging.info(f"Raw data saved to: {self.output_path}")

# Legacy/helper
def extract_weather_data(url: str) -> None:
    extractor = WeatherExtractor()
    extractor.extract(url)