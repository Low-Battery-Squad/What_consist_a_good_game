#### 4. Database Setup and Data Loading

This section describes how users can fully recreate the PostgreSQL database and load the cleaned dataset using the scripts inside the `Data_load` directory. The process is designed so that any group member—or any external user cloning the repository—can run the entire pipeline on their own machine.

---

#### 4.1 Create PostgreSQL Database

* **Create Database:**  
  Run the following command inside pgAdmin or psql:  
  `CREATE DATABASE steam_db;`

* **Purpose:**  
  This creates the dedicated database where all cleaned game data will be stored.

---

#### 4.2 Apply the Database Schema

* **Schema File Location:**  
  All database structures are defined in the schema file:  
  `sql/schema.sql`

* **Apply Schema:**  
  Execute the schema file using the following command:  
  `psql -d steam_db -f sql/schema.sql`

* **Result:**  
  This creates the `games` table, initializes JSONB fields, and builds all required indexes for efficient queries.

---

#### 4.3 Configure Environment Variables

The loader script uses environment variables stored in an `.env` file.

* **Step 1 — Open Template File:**  
  Navigate to the following file and fill in your PostgreSQL login details:  
  `Data_load/.env.example`

* **Step 2 — Copy Contents to .env:**  
  After entering your actual credentials in `.env.example`, copy the entire file content into a new file named:  
  `Data_load/.env`

* **Recommended Template Structure:**  
  PGHOST=localhost  
  PGPORT=5432  
  PGDATABASE=steam_db  
  PGUSER=your_username  
  PGPASSWORD=your_password

* **Purpose:**  
  These variables allow the loader script to read your database configuration automatically, without modifying the code.

---

#### 4.4 Install Python Requirements

* **Install Dependencies:**  
  Install all required Python libraries using:  
  `pip install -r requirements.txt`

* **Libraries Used:**  
  The script relies on `psycopg2`, `python-dotenv`, `pandas`, and other required packages included in the project.

---

#### 4.5 Load Cleaned Data into PostgreSQL

* **Run Loader Script:**  
  Navigate into the loading directory and execute:  
  `cd Data_load`  
  `python load_to_db.py`

* **The script will:**  
  * **Read Input:** Load the cleaned CSV from `Data_cleaning/data/processed/games_clean.csv`.  
  * **Parse Types:** Convert dates, booleans, integers, and floats into PostgreSQL-compatible formats.  
  * **Build JSONB:** Construct `genres_json` and `raw_data_json` for JSONB storage.  
  * **Reset Table:** Truncate the `games` table so each run represents the latest snapshot.  
  * **Insert Rows:** Insert all records using safe parameterized SQL (`execute_values`).  
  * **Validate Data:** Skip invalid rows automatically and ensure a clean transaction.

---

#### 4.6 Verify Data Import

* **Preview First Rows:**  
  `SELECT * FROM games LIMIT 20;`

* **Check Total Row Count:**  
  `SELECT COUNT(*) FROM games;`

These commands verify that the database was set up correctly and populated with the cleaned dataset.
