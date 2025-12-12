# What consist a good game?
**Group Members:** Linyao(Bob) Ni, Zhifang Luo, Zongshuai Shen, Jiajie Wang   
  
This repo implements a full data pipeline to study how different game features
(on Steam) are associated with **user review ratios** (positive_reviews / total_reviews).

## 1. Project Overview & Motivation

### 1.1 Motivation

### 1.2 Approach

1. Use Steam and SteamSpy APIs to sample a configurable set of games.
2. Construct a clean dataset with variables such as price, owners proxy,
   days since release, is_free, and genre dummies.
3. Store the data in a PostgreSQL database.
4. Estimate an OLS regression where the dependent variable is the **user review ratio**,
   and regressors include the game features above.

## 2. Data Sources & Key Variables

### 2.1 Data Sources

- **Steam Web API – GetAppList**  
  Used to obtain a large pool of `(appid, name)` for candidate games.

- **Steam Storefront – appdetails & appreviews**  
  Official metadata (release date, price, genres, is_free) and
  aggregated review counts (`total_reviews`, `total_positive`).

- **SteamSpy – appdetails**  
  Third-party owners range, e.g. `"200,000 .. 500,000"`,
  from which we compute a midpoint **`owners_proxy`** as a **popularity tier**
  rather than an exact sales number.

### 2.2 Unit of Observation & Key Variables

Each row in the final dataset corresponds to **one Steam game** at the time of scraping.

Key variables used in our regression:

- `review_ratio` – `positive_reviews / total_reviews`
- `original_price` – original price (USD)
- `owners_proxy` – midpoint of SteamSpy owners range
- `days_since_release` – days between release date and snapshot date
- `is_free` – indicator for free-to-play games
- `main_genre` and genre dummies
- `total_reviews` – also used as a possible weight / filter

## 3. Reproducibility: How to Rerun Everything

### 3.1 Prerequisites

1. **Python environment**

   From the project root:

       pip install -r requirements.txt

2. **Steam Web API key**

   - Request a free key from Steam.
   - In the project root (`What_consist_a_good_game/`), create `.env`:

         STEAM_API_KEY=your_steam_api_key_here

3. **PostgreSQL**

   - Install PostgreSQL and start the server.
   - Create a database, e.g. `steam_db`.
   - Either set environment variables in `.env` (used by `Data_load/load_to_db.py`):

         PGHOST=localhost
         PGPORT=5432
         PGDATABASE=steam_db
         PGUSER=postgres
         PGPASSWORD=your_pg_password

   - Once per machine, create the schema:

         psql -U <user> -h localhost -d steam_db -f sql/schema.sql

### 3.2 One-Command Pipeline

From the **project root**, run:
```bash
python main.py  
```
This master script sequentially runs:

1. Data_collection/main.py – data collection (interactive config)

2. Data_cleaning/main.py – cleaning & feature engineering

3. Data_load/load_to_db.py – load cleaned data into PostgreSQL

4. Data_analysis/visualization.py – regression and figures

If any step fails, the script stops and prints an error.

### 3.3 Data-Collection Configuration 

Step A will ask for a configuration tuple:

    (target_n, min_year, price_flag, sample_mode_flag, genre_string, max_candidates)

- `target_n` – number of games desired in the final sample  
- `min_year` – minimum release year (0 = no filter)  
- `price_flag` – 0: all, 1: free only, 2: paid only  
- `sample_mode_flag` – 0: random sample, 1: top by popularity  
- `genre_string` – genre filter (e.g. "Indie", "Action"), "" = no filter  
- `max_candidates` – 0: automatic soft limit; >0: manual limit; -1: no soft limit

Example configs:

- 500 random games, no filters: (500, 0, 0, 0, "", 0)
- Top 300 games released since 2020: (300, 2020, 0, 1, "", 0)
- Top 200 Indie games (any year): (200, 0, 0, 1, "Indie", 0)

---

## 4. Pipeline Details by Module

### 4.1 Module A – Data Collection (Member A)

TODO (A):
- Explain how APIs are called, sampling logic, and any filtering rules.  
- Document important design choices (e.g. popularity ranking, retry logic).  
- Describe the structure of `games_filtered.json`.

### 4.2 Module B – Data Cleaning & Feature Engineering (Member B)

TODO (B):
- Describe major cleaning rules (drop conditions, handling missing values).  
- Explain how `review_ratio`, `days_since_release`, `owners_proxy` variants,
  and `main_genre` / dummies are constructed.  
- Summarize the final `games_clean.csv` schema.

### 4.3 Module C – Database & SQL (Member C)

TODO (C):
- Describe the PostgreSQL schema (key columns + JSONB fields).  
- Explain how `load_to_db.py` works (truncate + bulk load).  
- Provide 1–2 example SQL queries that illustrate how the table can be used
  (e.g. average review_ratio by genre, top free games, etc.).

### 4.4 Module D – Regression, Visualization (Zhifang Luo)

The analysis centers on an Ordinary Least Squares (OLS) Regression Model specified to quantify the impact of price, total reviews, release age, and genre on the Review Ratio. The model was fitted using statsmodels and critically employed HC3 Robust Standard Errors (by setting cov_type='HC3') to correct for observed heteroscedasticity in the error terms, ensuring the reliability of hypothesis tests (P-values and confidence intervals). 

After we trying to adjust the hypothetical OLS model, we've found that there is **severe multicollinearity**. To address it (indicated by a large Condition Number), we optimized the model by removing specific high-collinearity genres `(g_Nudity, g_Sports)` and establishing the most frequent genre `(g_Action)` as the reference category. All final model results, including the coefficient table and comprehensive fit statistics, are output directly to the console via the print(model.summary()) command.

In addition to the text output, the analysis module generates three figures, all saved within the `Data_analysis/figures/ directory`. These artifacts provide visual documentation of the process and results: the Price vs. Review Ratio Scatter Plot (plot_price_optimized.png) shows the raw relationship between variables; the Residuals vs. Fitted Values Plot (plot_residuals_optimized.png) serves as a diagnostic tool for checking model assumptions; and the Genre Impact Bar Chart (plot_genre_impact.png) visually presents the calculated marginal effects (coefficients) and their 95% confidence intervals for all included genre categories.

See the README located in `Data_analysis` for more information.

---

## 5. Results & Summary

### 5.1 Main Regression Specification (Short)

Baseline model:

    review_ratio_i
        = β0
        + β1 * original_price_i
        + β2 * owners_proxy_i
        + β3 * days_since_release_i
        + β4 * is_free_i
        + Σ_k γ_k * genre_{ik}
        + ε_i

**Model Details:**
* **Weighting:** Ordinary Least Squares (OLS) was used; no Weighted Least Squares (WLS) was applied.
* **Robust Standard Errors (Robust SE):** The model was fitted using **HC3 Robust Standard Errors** (Heteroscedasticity Consistent 3) to correct for the presence of heteroscedasticity in the data.

### 5.2 Key Quantitative Results

1.  **Popularity Halo Effect:** **`total_reviews`** is the strongest positive predictor ($\beta_2 = 1.677e^{-7}$, $p=0.001$). This indicates that a higher volume of reviews strongly correlates with a higher positive review ratio, suggesting a significant "popularity halo effect."
2.  **Price Insignificance:** The coefficient for **`current_price`** ($0.0002$) is **not statistically significant** ($p=0.828$). Price itself has no discernible impact on the proportion of positive reviews.
3.  **Longevity Effect:** **`days_since_release`** shows a statistically significant positive effect ($\beta_3 = 1.203e^{-5}$, $p=0.007$). This likely reflects that games surviving the market and continuing to gain reviews over time tend to possess higher intrinsic quality.
4.  **Genre Advantage:** **`g_Racing`** exhibits the strongest positive genre effect ($\beta = 0.0866$, $p=0.000$). This means Racing games have a Review Ratio approximately 8.66 percentage points higher than the base category (Action), after controlling for price, reviews, and age. 

### 5.3 Figures & Interpretation

All figures are saved in the `Data_analysis/figures/` directory.

**plot_price_optimized.png (Scatter Plot)**: 
<img width="2000" height="1500" alt="image" src="https://github.com/user-attachments/assets/6273ef10-a6f2-4d8e-9e64-86d55ac0fad7" />
This plot displays the raw distribution of price and review ratio, visually confirming the weak linear correlation between them.

**plot_residuals_optimized.png (Residuals vs. Fitted Plot)**:
<img width="2000" height="1500" alt="image" src="https://github.com/user-attachments/assets/d9206908-76bc-468d-b016-c0ac58b8e201" />
We can learn that the model's error terms are randomly distributed, which is necessary to confirm the validity of the OLS assumptions.

**plot_genre_impact.png (Coefficient Plot)**:
<img width="2400" height="1200" alt="image" src="https://github.com/user-attachments/assets/9c68b46c-d2e0-47d1-affa-9caec1133051" />
The bar chart visually represents the estimated marginal impact (coefficients) of each genre relative to the base category, including error bars to show their statistical significance.

---

## 6. Limitations & Future Work

### 6.1 Data Limitations

TODO:
- SteamSpy owners are approximate and noisy.  
- Single snapshot in time (no panel structure).  
- Missing unobservable quality measures (e.g. gameplay depth, graphics, review text sentiment).

### 6.2 Modeling Limitations

TODO:
- Linear OLS may miss nonlinearities and interactions.  
- Potential endogeneity: better games might both sell more and receive better reviews.  
- Genre and price may proxy for many unobserved factors (marketing budget, production values).
