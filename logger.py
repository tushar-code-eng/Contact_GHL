import logging
import os
from datetime import datetime

os.makedirs("logs", exist_ok=True)

# Create timestamped log file
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join("logs", f"{timestamp}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log(msg):
    print(msg)
    logging.info(msg)

def error(msg):
    print(msg)
    logging.error(msg)