import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
from house_prices import FEATURE_COLUMNS, MODEL_PATH
from house_prices.preprocess import preprocess
from dotenv import load_dotenv
import joblib

load_dotenv()

# Preprocess task
def preprocess_task(**kwargs):
    # Simulating loading data
    data = pd.read_csv(os.getenv('DATA_PATH'))  # Example: path loaded from env
    processed_data = preprocess(data)  # Preprocessing the data
    output_path = os.path.join(os.getenv('OUTPUT_PATH'), 'processed_data.csv')
    processed_data.to_csv(output_path, index=False)
    return output_path


# Training task
def training_task(**kwargs):
    # Loading preprocessed data
    processed_data = pd.read_csv(kwargs['ti'].xcom_pull(task_ids='preprocess_task'))
    X = processed_data[FEATURE_COLUMNS]
    y = processed_data['price']  # Assuming 'price' is the target column
    
    # Placeholder for model training (e.g., sklearn)
    from sklearn.linear_model import LinearRegression
    model = LinearRegression()
    model.fit(X, y)

    # Saving the model
    joblib.dump(model, MODEL_PATH)


# Inference task
def inference_task(**kwargs):
    # Loading trained model
    model = joblib.load(MODEL_PATH)

    # Simulating inference data
    new_data = pd.read_csv(os.getenv('NEW_DATA_PATH'))
    processed_data = preprocess(new_data)
    X_new = processed_data[FEATURE_COLUMNS]

    # Predicting
    predictions = model.predict(X_new)
    
    output_path = os.path.join(os.getenv('OUTPUT_PATH'), 'predictions.csv')
    pd.DataFrame({'predictions': predictions}).to_csv(output_path, index=False)
    return output_path


default_args ={
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    'house_price_prediction_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    preprocess_task = PythonOperator(
        task_id = "preprocess_task",
        python_callable= preprocess_task,
        provide_context = True,
    )

    training_task = PythonOperator(
        task_id = "training_task",
        python_callable= training_task,
        provide_context = True,
    )

    inference_task = PythonOperator(
        task_id = "inference_task",
        python_callable= inference_task,
        provide_context = True,
    )


    preprocess_task >> training_task >> inference_task