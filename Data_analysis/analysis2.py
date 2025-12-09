import os
import psycopg2
import pandas as pd
import statsmodels.api as sm
from dotenv import load_dotenv


load_dotenv(os.path.join("Data_load", ".env"))


def get_conn():
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "steam_db")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
    )


def load_data():
    conn = get_conn()
    query = """
        SELECT
            review_ratio,
            original_price,
            owners_proxy,
            days_since_release,
            is_free,
            total_reviews
        FROM games
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def prepare_xy(df: pd.DataFrame):
    cols = [
        "review_ratio",
        "original_price",
        "owners_proxy",
        "days_since_release",
        "is_free",
        "total_reviews",
    ]
    df = df[cols].dropna()

    for c in ["review_ratio", "original_price", "owners_proxy",
              "days_since_release", "total_reviews"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["is_free"] = df["is_free"].astype(int)
    df = df.dropna()

    y = df["review_ratio"]
    X = df[["original_price", "owners_proxy",
            "days_since_release", "is_free", "total_reviews"]]
    X = sm.add_constant(X)

    return y, X


def main():
    df = load_data()
    y, X = prepare_xy(df)

    model = sm.OLS(y, X).fit()

    print("n:", len(y))
    print("R-squared:", round(model.rsquared, 3))
    print("Adj. R-squared:", round(model.rsquared_adj, 3))

    out_txt = os.path.join("Data_analysis", "regression_simple_results.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(model.summary().as_text())


if __name__ == "__main__":
    main()
