import os
import json
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# === 1. Load .env configuration ===

# Project root directory: .../What_consist_a_good_game
BASE_DIR = Path(__file__).resolve().parents[1]

# Explicitly load the .env file located in the project root directory
load_dotenv(BASE_DIR / ".env")


def get_connection():
    """
    Read PostgreSQL connection settings from .env
    and establish a database connection.
    """
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "steam_db")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD", "")

    if not user:
        raise RuntimeError(
            "PGUSER is not set. Please create a .env file from .env.example "
            "and fill in your PostgreSQL username."
        )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    return conn


def load_clean_data_to_db(csv_path: str) -> None:
    """
    Load the cleaned CSV file and insert its content into the `games` table
    in the `steam_db` PostgreSQL database.

    Steps:
    - Validate required columns
    - Convert data types
    - Construct JSONB fields
    - TRUNCATE TABLE before each import to keep data updated
    - Use parameterized SQL insertion (execute_values)
    """
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    # === 2. Verify that all required columns exist ===
    expected_cols = [
        "app_id",
        "name",
        "original_price_usd",
        "current_price_usd",
        "is_free",
        "owners_proxy",
        "total_reviews",
        "review_ratio",
        "days_since_release",
        "main_genre",
        "release_date",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    # === 3. Data type conversions ===

    # Convert release_date from string format ("Nov 16, 2009") to Python date
    df["release_date"] = pd.to_datetime(
        df["release_date"],
        format="%b %d, %Y",
        errors="coerce",
    ).dt.date

    # Convert is_free: 0/1 → boolean
    df["is_free"] = df["is_free"].astype(bool)

    # === 4. Construct JSONB fields ===

    # genres_json: store the main_genre as a list for JSONB compatibility
    df["genres_json"] = df["main_genre"].apply(
        lambda g: [g] if pd.notna(g) else []
    )

    # raw_data_json: store selected original fields as JSONB
    def build_raw(row):
        return {
            "original_price_usd": float(row["original_price_usd"])
            if not pd.isna(row["original_price_usd"])
            else None,
            "current_price_usd": float(row["current_price_usd"])
            if not pd.isna(row["current_price_usd"])
            else None,
            "main_genre": row["main_genre"],
            "is_free": bool(row["is_free"]),
        }

    df["raw_data_json"] = df.apply(build_raw, axis=1)

    # === 5. Connect to PostgreSQL and insert data ===

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 5.1 Clear the table before inserting new data
        print("Truncating table games...")
        cur.execute("TRUNCATE TABLE games;")

        # 5.2 SQL INSERT statement (column names must match schema.sql)
        insert_sql = """
            INSERT INTO games (
                app_id,
                name,
                release_date,
                original_price,
                current_price,
                review_ratio,
                owners_proxy,
                days_since_release,
                is_free,
                main_genre,
                total_reviews,
                genres_json,
                raw_data_json
            ) VALUES %s
        """

        # 5.3 Convert DataFrame rows into tuples for batch insertion
        records = []
        for _, row in df.iterrows():
            # release_date: NaT -> None
            rd = row["release_date"]
            if pd.isna(rd):
                rd = None

            # days_since_release: NaN -> None, otherwise int
            ds = row["days_since_release"]
            if pd.isna(ds):
                ds = None
            else:
                ds = int(round(float(ds)))

            # owners_proxy: NaN -> None, otherwise int
            owners = row["owners_proxy"]
            if pd.isna(owners):
                owners = None
            else:
                owners = int(owners)

            # total_reviews: NaN -> None, otherwise int
            total_reviews = row["total_reviews"]
            if pd.isna(total_reviews):
                total_reviews = None
            else:
                total_reviews = int(total_reviews)

            # prices and ratio: NaN -> None, otherwise float
            orig_price = row["original_price_usd"]
            if pd.isna(orig_price):
                orig_price = None
            else:
                orig_price = float(orig_price)

            curr_price = row["current_price_usd"]
            if pd.isna(curr_price):
                curr_price = None
            else:
                curr_price = float(curr_price)

            review_ratio = row["review_ratio"]
            if pd.isna(review_ratio):
                review_ratio = None
            else:
                review_ratio = float(review_ratio)

            record = (
                int(row["app_id"]),
                row["name"],
                rd,
                orig_price,
                curr_price,
                review_ratio,
                owners,
                ds,
                bool(row["is_free"]),
                row["main_genre"],
                total_reviews,
                json.dumps(row["genres_json"]),
                json.dumps(row["raw_data_json"]),
            )
            records.append(record)

        print(f"Inserting {len(records)} rows into games...")
        execute_values(cur, insert_sql, records)

        conn.commit()
        print("Done! Data successfully loaded into games.")
    except Exception as e:
        conn.rollback()
        print("Error loading data to DB:", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Compute the CSV path (relative to project root)
    csv_path = BASE_DIR / "Data_cleaning" / "data" / "processed" / "games_clean.csv"
    load_clean_data_to_db(str(csv_path))
