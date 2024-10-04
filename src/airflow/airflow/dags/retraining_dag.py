import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from sklearn.linear_model import LinearRegression
import joblib
import sys
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../..')
    )
)
from dotenv import load_dotenv
from house_prices.preprocess import preprocess
from house_prices import FEATURE_COLUMNS, LABEL_COLUMN
from house_prices.inference import make_predictions
from house_prices.train import build_model

load_dotenv()
# Define the directory to monitor
MONITOR_DIR = os.getenv("TRAIN_DIR")
SAVE_DIR = os.getenv("TRAINED_MODEL_DIR")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'retraining_dag',
    default_args=default_args,
    description='A DAG to retrain model on new data',
    schedule_interval=None,
)

def find_new_csv_file():
    # List all CSV files in the directory
    files = [f for f in os.listdir(MONITOR_DIR) if f.endswith('.csv')]
    if not files:
        raise FileNotFoundError("No CSV files found in the directory.")
    # Return the first CSV file found
    return os.path.join(MONITOR_DIR, files[0])

def train_and_evaluate_model(filepath):
    # Build and evaluate the model
    metrics = build_model(filepath)
    print(f"Model evaluation metrics: {metrics}")

    # Perform inference using the make_predictions function
    predictions = make_predictions(filepath)
    print(f"Inference results: {predictions}")

# Define the PythonOperator to find a new CSV file
find_csv_task = PythonOperator(
    task_id='find_csv_task',
    python_callable=find_new_csv_file,
    dag=dag,
)

# Define the PythonOperator to train and evaluate the model
train_model_task = PythonOperator(
    task_id='train_model_task',
    python_callable=train_and_evaluate_model,
    op_kwargs={'filepath': '{{ task_instance.xcom_pull(task_ids="find_csv_task") }}'},
    dag=dag,
)

# Set task dependencies
find_csv_task >> train_model_task