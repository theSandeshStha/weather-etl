# Weather ETL Pipeline with Custom Forecasting

This project implements an end-to-end ETL pipeline using Apache Airflow on the Astronomer platform. It extracts real-time weather data from the [Open-Meteo API](https://open-meteo.com/), transforms and loads it into a PostgreSQL database, and uses a custom linear regression model (built with scikit-learn) to forecast the next day's temperature.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Metrics & Impact](#metrics--impact)
- [License](#license)

## Features

- **Automated Data Ingestion:**  
  Extracts current weather data every 5 minutes (for testing) or daily (in production) from Open-Meteo API.
- **Data Transformation & Storage:**  
  Transforms raw API data and stores it in a PostgreSQL database with proper schema management.
- **Custom Forecasting:**  
  Utilizes a custom linear regression model to forecast next-day temperatures based on 30 days of historical data.
- **Modular Airflow DAG:**  
  Implements a modular DAG with clearly defined tasks for extraction, transformation, load, and prediction.

- **Built with Astronomer CLI:**  
  Uses `astro dev init` for local development and testing, making it easy to deploy and iterate.

## Architecture

The pipeline consists of the following tasks:

1. **Extract Weather Data:**  
   Uses an HTTP hook to query the Open-Meteo API for current weather data.

2. **Transform Weather Data:**  
   Processes the JSON response to extract key fields like temperature, wind speed, and wind direction.

3. **Load Weather Data:**  
   Inserts the transformed data into a PostgreSQL table (`weather_data`).

4. **Predict Next Day's Temperature:**  
   Queries the past 30 days of weather data, applies a simple linear regression model (using scikit-learn), and forecasts the temperature for the next day. The forecast is then stored in a separate table (`weather_forecast`).

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)

## Installation & Setup

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-username/weather-etl-pipeline.git
   cd weather-etl-pipeline
   ```

2. **Initialize the Astronomer Project:**

   Use the Astronomer CLI to initialize the project if you haven't already:

   ```bash
   astro dev init
   ```

3. **Install Python Dependencies:**

   Ensure your `requirements.txt` includes the following (and any other required packages):

   ```txt
   apache-airflow
   apache-airflow-providers-http
   apache-airflow-providers-postgres
   pandas
   numpy
   scikit-learn
   requests
   ```

   Then install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. **Start the Local Airflow Environment:**

   Launch your local Airflow environment with Astronomer:

   ```bash
   astro dev start
   ```

5. **Access the Airflow Web UI:**

   Navigate to [http://localhost:8080](http://localhost:8080) and log in with the default credentials (if set up).

6. **Configure Connections:**

   - **HTTP Connection (open_meteo_api):**  
     Set up an Airflow connection with the ID `open_meteo_api` pointing to `https://api.open-meteo.com`.
   - **PostgreSQL Connection (postgres_default):**  
     Ensure you have a PostgreSQL connection set up in Airflow with the connection ID `postgres_default`.

## Usage

- **DAG Overview:**  
  The DAG (`weather_etl_pipeline`) is scheduled to run every 5 minutes for testing purposes. Adjust the schedule to `@daily` for production use.

- **Tasks Execution:**  
  The DAG executes the following tasks in sequence:

  1. `extract_weather_data`
  2. `transform_weather_data`
  3. `load_weather_data`
  4. `predict_next_day_weather`

- **Viewing Results:**
  - **Raw Weather Data:** Stored in the `weather_data` table.
  - **Forecast Data:** Stored in the `weather_forecast` table.  
    You can query these tables using your preferred SQL client or through the Airflow PostgresHook logs.

## Configuration

- **DAG Parameters:**  
  Modify the DAG’s schedule interval and connection IDs as needed.

- **Model Customization:**  
  The forecasting model uses a simple linear regression. Feel free to adjust the feature engineering or replace it with a more complex model if required.

- **Environment Variables:**  
  Use Airflow variables or environment variables to manage API endpoints and credentials securely.
