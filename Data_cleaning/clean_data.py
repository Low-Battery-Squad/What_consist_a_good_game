import json
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any

def clean_raw_data(input_path: str, output_path: str) -> None:
    """
    Reads raw game data from a JSON file, performs data cleaning and 
    feature engineering, and saves the final dataset to a CSV file.

    Args:
        input_path (str): The relative path to the raw JSON data file.
        output_path (str): The relative path where the cleaned CSV data 
                           will be saved.
    
    Returns:
        None: The result is saved directly to a file.
    """
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {input_path}")
        return

    print(f"Initial raw data size: {len(df)} records.")

    MIN_REVIEWS = 50 
    
    df_clean = (df
        .dropna(subset=['release_date', 'total_reviews', 'owners_proxy', 'original_price_cents'])
        .copy()
    )
    df_clean = df_clean[df_clean['total_reviews'] >= MIN_REVIEWS]
    
    print(f"Data size after cleaning and filtering: {len(df_clean)} records.")
    
    
    df_clean['original_price_usd'] = df_clean['original_price_cents'] / 100
    df_clean['current_price_usd'] = df_clean['current_price_cents'] / 100

    df_clean['review_ratio'] = df_clean['positive_reviews'] / df_clean['total_reviews']
    
    snapshot_time_str = df_clean['snapshot_time'].iloc[0]
    snapshot_dt = datetime.fromisoformat(snapshot_time_str.replace('Z', '+00:00'))

    df_clean['release_dt'] = pd.to_datetime(df_clean['release_date'], errors='coerce')
    
    df_clean['days_since_release'] = (
        (snapshot_dt - df_clean['release_dt'])
        .dt.days
    )
    
    df_clean['main_genre'] = df_clean['genres'].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) > 0 else 'Unknown'
    )
    
    df_clean['is_free'] = df_clean['is_free'].astype(int)

    
    final_cols = [
        'app_id', 'name', 
        'original_price_usd', 'current_price_usd', 'is_free', 
        'owners_proxy', 'total_reviews', 'review_ratio',
        'days_since_release', 'main_genre', 'release_date'
    ]
    
    df_clean[final_cols].to_csv(output_path, index=False)
    
    print(f"Saved {len(df_clean)} cleaned records to {output_path}")


if __name__ == "__main__":
    INPUT_FILE = "Rawdata/games_raw.json" 
    OUTPUT_FILE = "data/processed/games_clean.csv"
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    clean_raw_data(INPUT_FILE, OUTPUT_FILE)
