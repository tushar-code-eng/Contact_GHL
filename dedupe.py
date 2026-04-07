import json
import os

FILE = "data/processed_ids.json"

def load_processed():
    if not os.path.exists(FILE):
        return set()
    with open(FILE, "r") as f:
        return set(json.load(f))

def save_processed(ids):
    with open(FILE, "w") as f:
        json.dump(list(ids), f)

def is_new(activity_id, processed_ids):
    return activity_id not in processed_ids