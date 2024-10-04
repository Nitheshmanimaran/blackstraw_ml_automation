from dotenv import load_dotenv
import os
# Load environment variables from .env file

load_dotenv()

scaler_path = os.getenv('SCALER_PATH')
print(f"scaler path is : {scaler_path}")