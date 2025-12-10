import os
import json
import pandas as pd
import numpy as np
import psycopg2
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

def get_db_conn():
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "steam_db")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD", "")

    if not user:
        raise RuntimeError("connection error")

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    return conn

def load_genres(data):
    if isinstance(data, list):
        return [g.strip() for g in data if isinstance(g, str) and g.strip()]
    elif isinstance(data, str):
        try:
            data = json.loads(data.strip())
            if isinstance(data, list):
                return [g.strip() for g in data if isinstance(g, str) and g.strip()]
        except json.JSONDecodeError:
            pass
    return []

def analyze_data():
    figures_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "figures")
    os.makedirs(figures_dir, exist_ok=True)

    Y_VAR = 'review_ratio'
    X_VARS_CONT = ['current_price', 'total_reviews', 'days_since_release']

    conn = None
    try:
        conn = get_db_conn()
        all_vars = [Y_VAR, 'app_id', 'genres_json'] + X_VARS_CONT
        query = f"SELECT {', '.join(all_vars)} FROM games WHERE total_reviews IS NOT NULL AND genres_json IS NOT NULL;"
        df = pd.read_sql(query, conn)
    except Exception as e:
        return
    finally:
        if conn:
            conn.close()

    df = df.dropna(subset=[Y_VAR] + X_VARS_CONT)

    df['g_list'] = df['genres_json'].apply(load_genres)
    df = df.drop(columns=['genres_json'])
    df_exp = df.explode('g_list')
    df_dum = pd.get_dummies(df_exp['g_list'], prefix='g', dtype=int)
    df_dum['app_id'] = df_exp['app_id']
    df_gen_final = df_dum.groupby('app_id').max().reset_index()

    df = df.drop(columns=['g_list'])
    df = df.merge(df_gen_final, on='app_id', how='left').fillna(0)

    UNSTABLE_GENRES = ['g_Nudity', 'g_Sports']

    DUMMY_VARS = [col for col in df.columns if col.startswith('g_') and col not in UNSTABLE_GENRES]

    if DUMMY_VARS:
        BASE_GENRE_COL = DUMMY_VARS[0]
        df = df.drop(columns=[BASE_GENRE_COL])
        DUMMY_VARS.pop(0)

    X_VARS_ALL = X_VARS_CONT + DUMMY_VARS

    Y = df[Y_VAR]
    X = sm.add_constant(df[X_VARS_ALL])

    model = sm.OLS(Y, X).fit(cov_type='HC3')
    print(model.summary())


    # Price&review
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=df[X_VARS_CONT[0]], y=Y, color='darkblue', alpha=0.6)
    plt.title('Price vs. Review Ratio')
    plt.xlabel(X_VARS_CONT[0])
    plt.ylabel(Y_VAR)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.savefig(os.path.join(figures_dir, 'plot_price_optimized.png'), dpi=300)
    plt.close()
    print("Scatter plot saved")

    # Residuals
    plt.figure(figsize=(8, 6))
    sns.residplot(x=model.fittedvalues, y=model.resid, lowess=True, line_kws={'color': 'red'})
    plt.title('Residuals vs. Fitted Values')
    plt.xlabel('Fitted Values')
    plt.ylabel('Residuals')
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.savefig(os.path.join(figures_dir, 'plot_residuals_optimized.png'), dpi=300)
    plt.close()
    print("Residuals plot saved")

    # Genre  Bar Chart
    ALL_GENRE_COLS_IN_MODEL = [col for col in model.params.index if col.startswith('g_')]

    genre_coeffs = model.params[ALL_GENRE_COLS_IN_MODEL]
    genre_conf_int = model.conf_int().loc[ALL_GENRE_COLS_IN_MODEL]
    
    errors = np.abs(genre_conf_int.T.values - genre_coeffs.values)

    plt.figure(figsize=(12, 7))

    labels = [col.replace('g_', '') for col in genre_coeffs.index]
    plt.bar(labels, genre_coeffs.values, yerr=errors, capsize=5, color='skyblue')

    plt.axhline(0, color='grey', linewidth=0.8)

    plt.title('Impact of Game Genres on Review Ratio')
    plt.xlabel('Genre')
    plt.ylabel('Coefficient (Change in Review Ratio)')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(figures_dir, 'plot_genre_impact.png'), dpi=300)
    plt.close()
    print("Bar chart saved")


if __name__ == "__main__":
    analyze_data()
