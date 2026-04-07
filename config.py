import os
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    "username": os.getenv("PRPT_USERNAME"),
    "password": os.getenv("PRPT_PASSWORD"),
    "ghl_api_token": os.getenv("GHL_API_TOKEN"),
    "ghl_location_id": os.getenv("GHL_LOCATION_ID", "ve9EPM428h8vShlRW1KT")  # Default location ID from sample
}