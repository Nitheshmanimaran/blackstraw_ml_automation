import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import joblib
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_log_error
from sklearn.model_selection import train_test_split
import numpy as np
import mlflow
import mlflow.sklearn



load_dotenv()
os.environ['MLFLOW_TRACKING_URI'] = 'http://localhost:5000'
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CONTINUOUS_FEATURE_COLUMNS = ['TotRmsAbvGrd', 'WoodDeckSF', 'YrSold', '1stFlrSF']
CATEGORICAL_FEATURE_COLUMNS = ['Foundation', 'KitchenQual']
FEATURE_COLUMNS = CONTINUOUS_FEATURE_COLUMNS + CATEGORICAL_FEATURE_COLUMNS
LABEL_COLUMN = 'SalePrice'
DATA_PATH = os.getenv('NEW_TRAIN_DIR')
TRAINED_DIR = os.getenv('TRAINED_DIR')
MODEL_PATH = os.getenv('TRAINED_MODEL_PATH')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'train_model_dag',
    default_args=default_args,
    description='A DAG to train models on new data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def check_files_task(**kwargs):
    logger.info("Checking for new CSV files")
    csv_files = [f for f in os.listdir(DATA_PATH) if f.endswith('.csv')]
    if not csv_files:
        logger.info("No new CSV files found")
        return None
    logger.info(f"Found CSV files: {csv_files}")
    return csv_files

def preprocess_task(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(task_ids='check_files_task')
    if not csv_files:
        return None

    for csv_file in csv_files:
        logger.info(f"Preprocessing file: {csv_file}")
        # Create a directory named after the CSV file (without extension)
        output_dir = os.path.join(TRAINED_DIR, os.path.splitext(csv_file)[0])
        os.makedirs(output_dir, exist_ok=True)
        df_raw = pd.read_csv(os.path.join(DATA_PATH, csv_file))

        # Log the columns in the DataFrame
        logger.info(f"Columns in DataFrame: {df_raw.columns.tolist()}")
        logger.info(f"Expected continuous columns: {CONTINUOUS_FEATURE_COLUMNS}")

        # Preprocess continuous features
        try:
            scaler = StandardScaler()
            scaler.fit(df_raw[CONTINUOUS_FEATURE_COLUMNS])
            joblib.dump(scaler, os.path.join(output_dir, 'scaler.joblib'))
            scaled_features = scaler.transform(df_raw[CONTINUOUS_FEATURE_COLUMNS])
            scaled_df = pd.DataFrame(scaled_features, columns=CONTINUOUS_FEATURE_COLUMNS, index=df_raw.index)
        except KeyError as e:
            logger.error(f"KeyError: {e}")
            return None

        # Preprocess categorical features
        one_hot_encoder = OneHotEncoder(handle_unknown='ignore', dtype='int')
        one_hot_encoder.fit(df_raw[CATEGORICAL_FEATURE_COLUMNS])
        joblib.dump(one_hot_encoder, os.path.join(output_dir, 'one_hot_encoder.joblib'))
        categorical_features = one_hot_encoder.transform(df_raw[CATEGORICAL_FEATURE_COLUMNS])
        categorical_df = pd.DataFrame.sparse.from_spmatrix(categorical_features,
                                                           columns=one_hot_encoder.get_feature_names_out(),
                                                           index=df_raw.index)

        # Combine features
        processed_df = pd.concat([scaled_df, categorical_df], axis=1)
        processed_df.to_csv(os.path.join(output_dir, 'processed_data.csv'))
        logger.info(f"Preprocessed data saved for file: {csv_file}")

def train_model_task(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(task_ids='check_files_task')
    if not csv_files:
        return None

    for csv_file in csv_files:
        logger.info(f"Training model for file: {csv_file}")
        output_dir = os.path.join(TRAINED_DIR, os.path.splitext(csv_file)[0])
        df = pd.read_csv(os.path.join(output_dir, 'processed_data.csv'))
        df_raw = pd.read_csv(os.path.join(DATA_PATH, csv_file))

        X_train, X_test, y_train, y_test = train_test_split(
            df,
            df_raw[LABEL_COLUMN],
            test_size=0.33,
            random_state=42
        )

        model = LinearRegression()

        # Start an MLflow run
        with mlflow.start_run():
            model.fit(X_train, y_train)
            joblib.dump(model, os.path.join(output_dir, 'model.joblib'))
            logger.info(f"Model trained and saved for file: {csv_file}")

            # Log model and parameters
            mlflow.log_param("csv_file", csv_file)
            mlflow.log_param("model_type", "LinearRegression")
            mlflow.sklearn.log_model(model, "model")

            # Evaluate and log metrics
            y_pred = model.predict(X_test)
            rmsle = np.sqrt(mean_squared_log_error(y_test, y_pred))
            mlflow.log_metric("rmsle", rmsle)
            logger.info(f"RMSLE for file {csv_file}: {rmsle}")

            # Log artifacts
            mlflow.log_artifact(os.path.join(output_dir, 'scaler.joblib'))
            mlflow.log_artifact(os.path.join(output_dir, 'one_hot_encoder.joblib'))

            # Register the model
            mlflow.sklearn.log_model(model, "model", registered_model_name="HousePriceModel")

        # Save test data for RMSLE computation
        np.save(os.path.join(output_dir, 'X_test.npy'), X_test)
        np.save(os.path.join(output_dir, 'y_test.npy'), y_test)

def compute_rmsle_task(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(task_ids='check_files_task')
    if not csv_files:
        return None

    for csv_file in csv_files:
        logger.info(f"Computing RMSLE for file: {csv_file}")
        output_dir = os.path.join(TRAINED_DIR, os.path.splitext(csv_file)[0])
        model = joblib.load(os.path.join(output_dir, 'model.joblib'))
        X_test = np.load(os.path.join(output_dir, 'X_test.npy'))
        y_test = np.load(os.path.join(output_dir, 'y_test.npy'))

        y_pred = model.predict(X_test)
        rmsle = np.sqrt(mean_squared_log_error(y_test, y_pred))
        logger.info(f"RMSLE for file {csv_file}: {rmsle}")

check_files = PythonOperator(
    task_id='check_files_task',
    python_callable=check_files_task,
    dag=dag,
)

preprocess = PythonOperator(
    task_id='preprocess_task',
    python_callable=preprocess_task,
    provide_context=True,
    dag=dag,
)

train_model = PythonOperator(
    task_id='train_model_task',
    python_callable=train_model_task,
    provide_context=True,
    dag=dag,
)

compute_rmsle = PythonOperator(
    task_id='compute_rmsle_task',
    python_callable=compute_rmsle_task,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_files >> preprocess >> train_model >> compute_rmsle