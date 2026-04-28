import json
import os
import hashlib

FILE = "data/processed_ids.json"
PROCESSED_HASHES_FILE = "data/processed_hashes.json"

def load_processed():
    if not os.path.exists(FILE):
        return set()
    try:
        with open(FILE, "r") as f:
            content = f.read().strip()
            if not content:
                return set()
            return set(json.loads(content))
    except json.JSONDecodeError:
        return set()

def save_processed(ids):
    with open(FILE, "w") as f:
        json.dump(list(ids), f)

def is_new(activity_id, processed_ids):
    return activity_id not in processed_ids


def compute_record_hash(record):
    """
    Compute a hash of record fields (excluding activity_id).
    Used to detect if a previously sent record has been updated.
    """
    # Create a copy and remove activity_id for hashing
    hash_dict = {k: v for k, v in record.items() if k != "activity_id"}
    # Convert to JSON string for consistent hashing
    hash_str = json.dumps(hash_dict, sort_keys=True, default=str)
    return hashlib.md5(hash_str.encode()).hexdigest()


def load_processed_hashes():
    """Load hashes of previously processed records"""
    if not os.path.exists(PROCESSED_HASHES_FILE):
        return {}
    try:
        with open(PROCESSED_HASHES_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_processed_hashes(hashes):
    """Save hashes of processed records"""
    with open(PROCESSED_HASHES_FILE, "w") as f:
        json.dump(hashes, f)


def has_record_changed(activity_id, record, processed_hashes):
    """
    Check if a previously processed record has been updated.
    Returns True if record is new or if hash differs (record changed).
    """
    current_hash = compute_record_hash(record)
    stored_hash = processed_hashes.get(activity_id)
    
    if stored_hash is None:
        return True  # New record
    
    return current_hash != stored_hash  # True if changed, False if same