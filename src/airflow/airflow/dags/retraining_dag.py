import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import mlflow
import mlflow.sklearn
from dotenv import load_dotenv
import sys
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../..')
    )
)
from house_prices import CATEGORICAL_FEATURE_COLUMNS, CONTINUOUS_FEATURE_COLUMNS, FEATURE_COLUMNS, LABEL_COLUMN
import joblib

load_dotenv()
MONITOR_DIR = os.getenv("TRAIN_DIR")
MODELS_DIR = os.getenv("NEW_ARTIFACTS_DIR")

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
    files = [f for f in os.listdir(MONITOR_DIR) if f.endswith('.csv')]
    if not files:
        raise FileNotFoundError("No CSV files found in the directory.")
    return os.path.join(MONITOR_DIR, files[0])

def preprocess_data(filepath):
    # Load new data
    df = pd.read_csv(filepath)

    # Load the encoders
    scaler = joblib.load(os.path.join(MODELS_DIR, 'scaler.joblib'))
    one_hot_encoder = joblib.load(os.path.join(MODELS_DIR, 'one_hot_encoder.joblib'))

    # Preprocess continuous features
    scaled_features = scaler.transform(df[CONTINUOUS_FEATURE_COLUMNS])
    scaled_df = pd.DataFrame(scaled_features, columns=CONTINUOUS_FEATURE_COLUMNS, index=df.index)

    # Preprocess categorical features
    categorical_features = one_hot_encoder.transform(df[CATEGORICAL_FEATURE_COLUMNS])
    categorical_df = pd.DataFrame.sparse.from_spmatrix(categorical_features,
                                                       columns=one_hot_encoder.get_feature_names_out(),
                                                       index=df.index)

    # Combine features
    processed_df = pd.concat([scaled_df, categorical_df], axis=1)
    return processed_df, df[LABEL_COLUMN]

def retrain_model(filepath):
    # Preprocess the data
    X, y = preprocess_data(filepath)

    # Load the latest model from MLflow
    model_uri = "models:/HousePriceModel/Production"
    model = mlflow.sklearn.load_model(model_uri)

    # Retrain the model
    model.fit(X, y)

    training_score = model.score(X, y)

    # Log the updated model as a new version
    with mlflow.start_run():
        mlflow.sklearn.log_model(model, "model", registered_model_name="HousePriceModel")
        # Optionally log metrics and parameters
        mlflow.log_metric("Train_metric", training_score)

    print("Model retrained and new version saved in MLflow.")

find_csv_task = PythonOperator(
    task_id='find_csv_task',
    python_callable=find_new_csv_file,
    dag=dag,
)

retrain_model_task = PythonOperator(
    task_id='retrain_model_task',
    python_callable=retrain_model,
    op_kwargs={'filepath': '{{ task_instance.xcom_pull(task_ids="find_csv_task") }}'},
    dag=dag,
)

find_csv_task >> retrain_model_task