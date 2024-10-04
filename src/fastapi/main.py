from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
import os
from dotenv import load_dotenv

load_dotenv()

SCALER_PATH = os.getenv('SCALER_PATH')
ONE_HOT_ENCODER_PATH = os.getenv('ONE_HOT_ENCODER_PATH')
MODEL_PATH = os.getenv('MODEL_PATH')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Load the scaler, one-hot encoder, and model
scaler = joblib.load(SCALER_PATH)
one_hot_encoder = joblib.load(ONE_HOT_ENCODER_PATH)
model = joblib.load(MODEL_PATH)

# Database connection
def get_db_connection():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

app = FastAPI()

class HouseFeatures(BaseModel):
    TotRmsAbvGrd: int
    WoodDeckSF: int
    YrSold: int
    FirstFlrSF: int
    Foundation: str
    KitchenQual: str

@app.post("/predict")
def predict(features: HouseFeatures):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Prepare the input data
    continuous_features = np.array([[features.TotRmsAbvGrd, features.WoodDeckSF, features.YrSold, features.FirstFlrSF]])
    categorical_features = np.array([[features.Foundation, features.KitchenQual]])
    
    # Scale continuous features
    scaled_continuous_features = scaler.transform(continuous_features)
    
    # One-hot encode categorical features
    encoded_categorical_features = one_hot_encoder.transform(categorical_features).toarray()
    
    # Combine features
    combined_features = np.hstack((scaled_continuous_features, encoded_categorical_features))
    
    # Make prediction
    prediction = model.predict(combined_features)
    
    # Convert prediction to Python float with two decimal points
    sale_price = round(float(prediction[0]), 2)
    
    # Get current timestamp
    prediction_timestamp = datetime.now()
    
    # Insert prediction into the database
    cursor.execute(
        "INSERT INTO public.api_predictions (TotRmsAbvGrd, WoodDeckSF, YrSold, FirstFlrSF, Foundation, KitchenQual, SalePrice, PredictionTimestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (features.TotRmsAbvGrd, features.WoodDeckSF, features.YrSold, features.FirstFlrSF, features.Foundation, features.KitchenQual, sale_price, prediction_timestamp)
    )
    conn.commit()
    cursor.close()
    conn.close()
    
    return {"SalePrice": f"{sale_price:.2f}", "PredictionTimestamp": prediction_timestamp}

@app.get("/check_db")
def check_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {"status": "Database connection successful"}
    except Exception as e:
        return {"status": "Database connection failed", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)