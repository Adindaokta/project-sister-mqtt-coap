import csv
import os
import time

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def log_event(filename, message_id, sequence, event_type, timestamp=None, status="OK", detail=""):
    filepath = os.path.join(LOG_DIR, filename)
    file_exists = os.path.isfile(filepath)
    ts = timestamp if timestamp is not None else time.time()
    
    with open(filepath, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["message_id", "sequence", "event_type", "timestamp", "status", "detail"])
        writer.writerow([message_id, sequence, event_type, f"{ts:.6f}", status, detail])