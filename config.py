import os
import json
from dotenv import load_dotenv

load_dotenv()

def parse_max_records_config():
    """
    Parse MAX_RECORDS_TO_PUSH_TO_GHL from environment.
    Expected format: {"tag_name": number, "tag_name2": number}
    Example: {"quoted_tag": 10, "completed_tag": 5}
    """
    config_str = os.getenv("MAX_RECORDS_TO_PUSH_TO_GHL", "")
    
    if not config_str or config_str.strip() == "":
        return None  # No limit, push all records
    
    try:
        return json.loads(config_str)
    except json.JSONDecodeError as e:
        print(f"⚠️ Error parsing MAX_RECORDS_TO_PUSH_TO_GHL: {e}")
        return None

CONFIG = {
    "username": os.getenv("PRPT_USERNAME"),
    "password": os.getenv("PRPT_PASSWORD"),
    "ghl_api_token": os.getenv("GHL_API_TOKEN"),
    "ghl_location_id": os.getenv("GHL_LOCATION_ID", "ve9EPM428h8vShlRW1KT"),  # Default location ID from sample
    "max_records_to_push": parse_max_records_config()
}