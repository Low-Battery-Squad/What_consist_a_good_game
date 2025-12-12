'''
Full project pipeline entry point.

Before running this file, please make sure:

1. Python environment
   - Python 3.x is installed.
   - From the project root, install dependencies:
       pip install -r requirements.txt

2. Steam Web API key
   - Request a free Steam Web API key from Steam.
   - In the project root, create a file named ".env" containing at least:
       STEAM_API_KEY=your_steam_api_key_here

3. PostgreSQL setup
   - PostgreSQL server is installed and running on your machine.
   - Create a database for this project (we assume the name "steam_db").
   - In the project root ".env" file, also set the database connection variables
     used by Data_load/load_to_db.py, for example:
       PGHOST=localhost
       PGPORT=5432
       PGDATABASE=steam_db
       PGUSER=your_pg_username
       PGPASSWORD=your_pg_password
   - Make sure the schema is created by running (once) in a terminal:
       psql -U <user> -h localhost -d steam_db -f sql/schema.sql
     (Adjust user/host/port/db name as needed.)

After these preparations, you can run the entire pipeline from the project root with:
    python main.py

This will sequentially execute:
  A: Data_collection/main.py       (API scraping, requires user config input)
  B: Data_cleaning/main.py        (cleaning & feature engineering)
  C: Data_load/load_to_db.py      (load cleaned data into PostgreSQL)
  D: Data_analysis/visualization.py (regression and plots)
'''

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def run_step(subdir: str, script: str, desc: str) -> None:
    """Run a python script in a given subdirectory with a short description."""
    print(f"\n=== {desc} ===")
    workdir = BASE_DIR / subdir
    cmd = [sys.executable, script]

    result = subprocess.run(cmd, cwd=workdir)

    if result.returncode != 0:
        print(f"\n[ERROR] Step failed: {desc} (exit code {result.returncode})")
        sys.exit(result.returncode)
    else:
        print(f"[OK] Finished: {desc}")


def main() -> None:
    print("=== Full pipeline: collection -> cleaning -> DB load -> analysis ===")

    # A: Data collection (will still ask you for the config tuple)
    run_step("Data_collection", "main.py", "A: Data collection")

    # B: Data cleaning & feature engineering
    run_step("Data_cleaning", "main.py", "B: Data cleaning")

    # C: Load cleaned data into PostgreSQL
    run_step("Data_load", "load_to_db.py", "C: Load data to PostgreSQL")

    # D: Regression / figures (adjust script name if your analysis entry changes)
    run_step("Data_analysis", "visualization.py", "D: Regression & figures")

    print("\n=== All steps completed successfully ===")


if __name__ == "__main__":
    main()
