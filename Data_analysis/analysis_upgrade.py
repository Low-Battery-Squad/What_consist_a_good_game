import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

FIGURES_DIR = BASE_DIR / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

def get_connection():
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "steam_db")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD", "")

    if not user:
        raise RuntimeError("PGUSER is not set. Check .env file.")

    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

def fetch_latest_data() -> pd.DataFrame:
    conn = get_connection()
    
    query = """
    SELECT
        review_ratio, original_price, owners_proxy, 
        days_since_release, is_free, main_genre, total_reviews
    FROM 
        games
    WHERE
        total_reviews >= 500;
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    df = df.dropna(subset=['review_ratio', 'original_price', 'main_genre', 'total_reviews'])
    
    return df

def run_regression_analysis(df: pd.DataFrame):
    
    formula = (
        "review_ratio ~ original_price + owners_proxy + days_since_release + is_free + C(main_genre)"
    )
    
    ols_model = smf.ols(formula=formula, data=df).fit() #OLS
    
    weights = df['total_reviews'].values #WLS
    wls_model = smf.wls(formula=formula, data=df, weights=weights).fit()
    
    print("OLS Coefficient Summary")
    ols_summary = ols_model.summary().tables[1].as_text()
    print(ols_summary)
    
    print("WLS Coefficient Summary")
    wls_summary = wls_model.summary().tables[1].as_text()
    print(wls_summary)
    
    with open(BASE_DIR / "ols_results_full.txt", "w") as f:
        f.write(ols_model.summary().as_text())

def generate_figures(df: pd.DataFrame):
    sns.set_theme(style="whitegrid")
    
    # Figure1: Price & Review Ratio (Scatter)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df, 
        x="original_price", 
        y="review_ratio", 
        hue="is_free", 
        alpha=0.6
    )
    plt.title("Review Ratio vs. Original Price")
    plt.savefig(FIGURES_DIR / "price_vs_review_ratio.png")
    plt.close()
    
    # Figure2: Avg review by Genre (Bar)
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df, 
        x="main_genre", 
        y="review_ratio", 
        errorbar=('ci', 95)
    )
    plt.title("Average Review Ratio by Main Genre")
    plt.xticks(rotation=45, ha='right') 
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "genre_avg_review_ratio.png")
    plt.close()
    
    print("\nFigures saved to figures/ directory.")


if __name__ == '__main__':
    data = fetch_latest_data()
    
    if not data.empty:
        run_regression_analysis(data)
        generate_figures(data)
    else:
        print("empty data, something wrong")
