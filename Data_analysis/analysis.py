import os

import numpy as np
import pandas as pd
import psycopg2
import statsmodels.api as sm
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

# connect with PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
)

# load data
query = """
    SELECT
        review_ratio,
        original_price,
        owners_proxy,
        days_since_release,
        is_free,
        main_genre,
        total_reviews
    FROM games
    WHERE review_ratio IS NOT NULL
      AND original_price IS NOT NULL;
"""
df = pd.read_sql(query, conn)
conn.close()

num_cols = [
    "review_ratio",
    "original_price",
    "owners_proxy",
    "days_since_release",
    "total_reviews",
]
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df["is_free"] = df["is_free"].astype(int)
df = df.dropna(subset=num_cols)
df = df[df["total_reviews"] >= 50].copy()
df["paid"] = (df["is_free"] == 0).astype(int)

# regression
reg = df[df["original_price"] > 0].copy()
reg["log_price"] = np.log(reg["original_price"])
reg["log_owners"] = np.log1p(reg["owners_proxy"])

genre_dummies = pd.get_dummies(reg["main_genre"], prefix="genre", drop_first=True)

X_df = pd.concat(
    [
        reg[["log_price", "log_owners", "days_since_release", "paid"]],
        genre_dummies,
    ],
    axis=1,
)
y_ser = reg["review_ratio"]

X_df = X_df.apply(pd.to_numeric, errors="coerce")
y_ser = pd.to_numeric(y_ser, errors="coerce")
mask = ~X_df.isna().any(axis=1) & ~y_ser.isna()
X_df = X_df.loc[mask]
y_ser = y_ser.loc[mask]

os.makedirs("Data_analysis", exist_ok=True)
os.makedirs("Data_analysis/figures", exist_ok=True)

if X_df.shape[0] < 10:
    with open("Data_analysis/regression_results.txt", "w") as f:
        f.write("Not enough observations for regression.\n")
    print("Not enough observations for regression.")
else:
    X_mat = X_df.to_numpy(dtype=float)
    y_vec = y_ser.to_numpy(dtype=float)
    X_mat = sm.add_constant(X_mat)

    # OLS
    model = sm.OLS(y_vec, X_mat).fit()

    var_names = ["const"] + list(X_df.columns)

    with open("Data_analysis/regression_results.txt", "w") as f:
        f.write(model.summary().as_text())

    n_params = len(model.params)
    var_names = [f"x{i}" for i in range(n_params)]

    coef_table = pd.DataFrame(
        {
            "variable": var_names,
            "coef": model.params,
            "pvalue": model.pvalues,
        }
    )
    coef_table.to_csv("Data_analysis/regression_coefs.csv", index=False)

    # create figures

    sub = df[(df["original_price"] > 0) & (df["total_reviews"] >= 50)].copy()
    x = sub["original_price"].to_numpy(dtype=float)
    y_scatter = sub["review_ratio"].to_numpy(dtype=float)

    mask_xy = np.isfinite(x) & np.isfinite(y_scatter)
    x = x[mask_xy]
    y_scatter = y_scatter[mask_xy]

    if x.size > 0:
        lowess = sm.nonparametric.lowess
        z = lowess(y_scatter, x, frac=0.3)

        plt.figure(figsize=(8, 6))
        plt.scatter(x, y_scatter, alpha=0.3)
        plt.plot(z[:, 0], z[:, 1], linewidth=2.0)
        plt.xlabel("Original Price")
        plt.ylabel("Review Ratio")
        plt.title("Review Ratio vs Price (LOWESS, reviews ≥ 50)")
        plt.tight_layout()
        plt.savefig("Data_analysis/figures/review_vs_price_lowess.png")
        plt.close()

    counts = df["main_genre"].value_counts()
    keep_genres = counts[counts >= 10].index
    g = (
        df[df["main_genre"].isin(keep_genres)]
        .groupby("main_genre")["review_ratio"]
        .mean()
        .sort_values()
    )

    if not g.empty:
        plt.figure(figsize=(10, 6))
        g.plot(kind="bar")
        plt.ylabel("Average Review Ratio")
        plt.xlabel("Main Genre")
        plt.xticks(rotation=45, ha="right")
        plt.title("Average Review Ratio by Main Genre (≥ 10 games)")
        plt.tight_layout()
        plt.savefig("Data_analysis/figures/avg_review_by_genre.png")
        plt.close()

    # residuals & fitted
    fitted = model.fittedvalues
    resid = model.resid

    plt.figure(figsize=(8, 6))
    plt.scatter(fitted, resid, alpha=0.4)
    plt.axhline(0, linewidth=1)
    plt.xlabel("Fitted values")
    plt.ylabel("Residuals")
    plt.title("Residuals vs Fitted")
    plt.tight_layout()
    plt.savefig("Data_analysis/figures/residuals_vs_fitted.png")
    plt.close()

    print("R-squared:", model.rsquared)
    print("Adj. R-squared:", model.rsquared_adj)
    print("Number of observations:", int(model.nobs))

print("Done.")
