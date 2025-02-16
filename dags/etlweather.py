from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from sklearn.linear_model import LinearRegression

LATITUDE = '27.662821'
LONGITUDE = '85.299510'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args,
         schedule_interval = '*/5 * * * *', #this for every 5 minutes, can also do timedelta(minutes=5), @daily for daily
         catchup = False) as dag:
    
    @task()
    def extract_weather_data(): # The E

        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')

        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=True'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data): # The T

        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

        return transformed_data
    
    @task()
    def load_weather_data(transformed_data): # The L
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    @task()
    def predict_next_day_weather():
        """
        Predict next day's temperature using a simple linear regression model built from the last 30 days of data.
        This is a custom forecasting approach without using Prophet.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()

        # Retrieve historical temperature data for the past 30 days
        query = """
            SELECT timestamp, temperature
            FROM weather_data
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            ORDER BY timestamp ASC;
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty or len(df) < 2:
            raise ValueError("Not enough historical data to forecast.")

        # Convert timestamp column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Create a numeric feature from the timestamp (Unix timestamp)
        df['ts_numeric'] = df['timestamp'].apply(lambda x: x.timestamp())

        X = df['ts_numeric'].values.reshape(-1, 1)
        y = df['temperature'].values

        model = LinearRegression()
        model.fit(X, y)

        # Determine the timestamp for the next day (24 hours after the latest record)
        last_timestamp = df['timestamp'].max()
        next_day_timestamp = last_timestamp + timedelta(days=1)
        next_day_numeric = np.array([[next_day_timestamp.timestamp()]])

        predicted_temp = model.predict(next_day_numeric)[0]

        pg_hook.run("""
        CREATE TABLE IF NOT EXISTS weather_forecast (
            forecast_timestamp TIMESTAMP,
            temperature_forecast FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        pg_hook.run("""
        INSERT INTO weather_forecast (forecast_timestamp, temperature_forecast)
        VALUES (%s, %s)
        """, parameters=(
            next_day_timestamp,
            predicted_temp
        ))

        forecast_info = {
            'forecast_timestamp': next_day_timestamp.isoformat(),
            'predicted_temperature': predicted_temp
        }
        return forecast_info


    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)
    forecast = predict_next_day_weather()