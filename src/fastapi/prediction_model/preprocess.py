from sklearn.model_selection import train_test_split
import joblib
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

SCALER_PATH = os.getenv('SCALER_PATH')
ONE_HOT_ENCODER_PATH = os.getenv('ONE_HOT_ENCODER_PATH')
MODEL_PATH = os.getenv('MODEL_PATH')


def split_dataset(data: pd.DataFrame, label: str):
    X, y = data.drop([label], axis=1), data[label]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.33,
        random_state=42
    )
    return X_train, X_test, y_train, y_test


def filter_useless_columns(X: pd.DataFrame) -> pd.DataFrame:
    useful_features = [
        'Foundation',
        'KitchenQual',
        'TotRmsAbvGrd',
        'WoodDeckSF',
        'YrSold',
        '1stFlrSF'
    ]
    X = X[useful_features]
    return X


def scale_continuous_features(X: pd.DataFrame):
    continuous_columns = [
        'TotRmsAbvGrd',
        'WoodDeckSF',
        'YrSold',
        '1stFlrSF'
    ]
    scaler = joblib.load(SCALER_PATH)
    scaled_columns = scaler.transform(X[continuous_columns])
    continuous_features = pd.DataFrame(
        data=scaled_columns,
        columns=continuous_columns
    )
    return continuous_features


def convert_categorical_values(X: pd.DataFrame) -> pd.DataFrame:
    categorical_columns = [
        "Foundation",
        "KitchenQual"
    ]
    ohe = joblib.load(ONE_HOT_ENCODER_PATH)
    array_hot_encoded = ohe.transform(X[categorical_columns])
    feature_columns = ohe.get_feature_names_out()
    categorical_features = pd.DataFrame.sparse.from_spmatrix(
        array_hot_encoded,
        columns=feature_columns
    )
    return categorical_features


def scale_convert_features(X: pd.DataFrame) -> pd.DataFrame:
    continuous_features = scale_continuous_features(X)
    categorical_features = convert_categorical_values(X)
    X_joined = continuous_features.join(categorical_features)
    return X_joined


def preprocess(X: pd.DataFrame, y: np.array):
    X = filter_useless_columns(X)
    X = scale_convert_features(X)
    return X, y
