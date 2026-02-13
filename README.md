# ETLDoTempo - Weather Data Pipeline

A professional, scalable ETL (Extract, Transform, Load) pipeline designed to fetch, process, and store weather data. This project leverages modern data engineering tools like **PySpark** for transformation and **Apache Airflow** for orchestration, following clean code principles and the **Inversion of Control (IoC)** design pattern.

## Overview

This pipeline automates the lifecycle of weather data:
1.  **Extraction**: Fetches real-time weather data from an external API and persists raw JSON data.
2.  **Transformation**: Utilizes PySpark to flatten nested JSON structures, normalize data types (especially datetimes), and rename columns for downstream compatibility.
3.  **Loading**: Synchronizes the processed data into a PostgreSQL database using Spark's native JDBC connector.

## Tech Stack

- **Language**: [Python 3.11+](https://www.python.org/)
- **Data Processing**: [PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
- **Orchestration**: [Apache Airflow](https://airflow.apache.org/)
- **Database**: [PostgreSQL](https://www.postgresql.org/)
- **Containerization**: [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- **Dependency Management**: [uv](https://github.com/astral-sh/uv)

## Project Structure

```text
ETLDoTempo/
├── dag/                  # Airflow DAG definitions
│   └── weather_dag.py    # Main ETL orchestration script
├── data/                 # Local storage for raw/intermediate data
├── src/                  # Core logic
│   ├── interfaces/       # Abstract Base Classes (IoC)
│   ├── extract_data.py   # API extraction logic
│   ├── transform_data.py # PySpark transformation logic
│   └── load_data.py      # Database loading logic
├── main.py               # Entry point for local execution
├── docker-compose.yml    # Airflow & Postgres environment setup
└── pyproject.toml        # Project dependencies and metadata
```

## Setup & Installation

### 1. Prerequisites
- [uv](https://github.com/astral-sh/uv) installed.
- Docker and Docker Compose installed.

### 2. Environment Configuration
Create a `.env` file in the `config/` directory (or root, depending on your setup) with the following variables:
```env
API_KEY=your_weather_api_key
url=https://api.openweathermap.org/data/2.5/weather?q=Sao+Paulo&appid=
user=airflow
password=airflow
database=airflow
```

### 3. Install Dependencies
Using `uv` to manage the virtual environment:
```bash
uv venv
source .venv/bin/activate
uv pip install -r pyproject.toml
```

### 4. Spin up the Environment
To start Airflow, PostgreSQL, and Redis:
```bash
docker-compose up -d
```

## Usage

### Local Execution
You can run the ETL pipeline directly via the `main.py` script for testing:
```bash
python main.py
```

### Airflow Orchestration
1. Access the Airflow UI at `http://localhost:8080`.
2. Enable the `weather_pipeline` DAG.
3. The pipeline is scheduled to run hourly (`0 */1 * * *`).

## Design Patterns
The project implements **Inversion of Control (IoC)** through abstract interfaces defined in `src/interfaces/`. This allows for easy swapping of components (e.g., changing the loader from Postgres to S3) without modifying the core pipeline orchestration logic.

## License
This project is licensed under the MIT License.

## Trouble
Looks like there's an error and the code broke. But I had nothing to do with it!
Just kidding. Open a new branch, submit the code, and let me know.
