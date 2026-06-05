```markdown
# Weather ETL Pipeline with Apache Airflow

An automated ETL pipeline that extracts real-time weather data 
from OpenWeatherMap API, transforms it, and loads it to AWS S3 
using Apache Airflow orchestrated on AWS EC2.

## Tools Used
- Apache Airflow
- AWS EC2
- AWS S3
- Python
- Pandas

## Pipeline Overview
1. **Extract** : Fetches real-time weather data from OpenWeatherMap API
2. **Transform** : Converts temperature from Kelvin to Fahrenheit and structures the data
3. **Load** : Saves the transformed data as CSV files to AWS S3

## Setup
- Deployed on AWS EC2 (Ubuntu)
- Orchestrated with Apache Airflow
- Data stored in AWS S3

<img width="1280" height="479" alt="image" src="https://github.com/user-attachments/assets/d9732d3d-c5f9-4424-9dd9-b5530a1c3b20" />
```
