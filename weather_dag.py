from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def kelvin_to_fahrenheit(kelvin):    
    temp_in_fahrenheit = (kelvin - 273.15) * 9/5 + 32
    return temp_in_fahrenheit

def transform_load_weather_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
    weather = data['weather'][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data['main']['temp'])
    feels_like_farenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
    min_temp_farenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
    max_temp_farenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt']== + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {'City': city,
                        'Description': weather,
                        'Temperature (F)': temp_farenheit,
                        'Feels Like (F)': feels_like_farenheit,
                        'Minimum Temp (F)': min_temp_farenheit,
                        'Maximum Temp(F)': max_temp_farenheit,
                        'Pressure': pressure,
                        'Humidity': humidity,
                        'Wind Speed (m/s)': wind_speed,
                        'Time of Record': time_of_record,
                        'Sunrise (Local Time)': sunrise_time,
                        'Sunset (Local Time)': sunset_time
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://weatherapiairflowbucket-test/{dt_string}.csv", index=False)

default_args = {
    'owner': 'salako',
    'depends_on_past': False,
    'email': ['salakonuel@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 17),
}

with DAG(
        'weather_dag',
        default_args=default_args,
        schedule='@daily',
        catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=e8dba1555a4c2a97dd96ee7c2fee1d52'
        )

        extract_weather_data = HttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=e8dba1555a4c2a97dd96ee7c2fee1d52',
        method = 'Get',
        response_filter = lambda r: json.loads(r.text),
        log_response = True
        )

        transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable = transform_load_weather_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

