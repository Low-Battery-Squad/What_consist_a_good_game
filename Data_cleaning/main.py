# main.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Data_cleaning.clean_data import clean_raw_data 

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RAW_DATA_PATH = os.path.join(BASE_DIR, "Data_collection", "Rawdata", "games_filtered.json")
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "Data_cleaning", "data", "processed", "games_clean.csv")

if __name__ == "__main__":
    
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)

    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)

    print("\n--- Cleaning and Feature Engineering ---")
    clean_raw_data(RAW_DATA_PATH, CLEAN_DATA_PATH)
    
    print("\n--- Complete ---")
