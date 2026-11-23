# logger_utils.py
import csv
import os

def init_csv(filename, headers):
    if not os.path.exists("logs"):
        os.makedirs("logs")
    filepath = os.path.join("logs", filename)
    if not os.path.exists(filepath):
        with open(filepath, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
    return filepath

def log_event(filepath, row_data):
    with open(filepath, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row_data)