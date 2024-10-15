from dotenv import load_dotenv
import os

def test_load_env():
    load_dotenv()
    scaler_path = os.getenv('SCALER_PATH')
    assert scaler_path is not None, "SCALER_PATH not set in environment variables"