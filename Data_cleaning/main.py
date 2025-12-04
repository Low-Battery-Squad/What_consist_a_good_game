# main.py

import os
from Data_collection.fetch_raw_data import fetch_and_save_raw_data 
from Data_cleaning.clean_data import clean_raw_data 

RAW_DATA_PATH = "Rawdata/games_raw.json"
CLEAN_DATA_PATH = "data/processed/games_clean.csv"

if __name__ == "__main__":
    
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)

    print("--- STEP 1: Fetching Raw Data ---")
    fetch_and_save_raw_data(RAW_DATA_PATH)

    print("\n--- STEP 2: Cleaning and Feature Engineering ---")
    clean_raw_data(RAW_DATA_PATH, CLEAN_DATA_PATH)
    
    print("\n--- Complete ---")
