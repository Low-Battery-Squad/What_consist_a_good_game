#### 1. Data Sources and Collection
* **Source:** Data was collected primarily from public Steam Web APIs (e.g., app details, review summaries) and the SteamSpy API.

* **Methodology:** We sampled a list of App IDs via the Steam API. For each ID, we performed subsequent calls to fetch detailed game information, aggregate review statistics, and a sales proxy (owners_proxy). All collected data was timestamped (snapshot_time) and stored as JSON (Rawdata/games_raw.json).

#### 2. Data Cleaning and Filtering
To ensure data quality for modeling, the following filtering criteria were applied:

* **Completeness Check:** Records were dropped if they were missing values for critical features: release_date, total_reviews, owners_proxy, or original_price_cents.

* **Minimum Review Threshold:** Games with fewer than 50 total reviews were excluded. This ensures that calculated review metrics are based on a stable sample size.

* **Type Filtering:** The collection process was designed to only retrieve records classified as "game", excluding other content types like software or DLC.

#### 3. Feature Engineering and Construction

The following variables were derived or processed from the cleaned data:

| Variable Name | Construction Method | Purpose/Meaning |
| :--- | :--- | :--- |
| **`review_ratio`** | `positive_reviews / total_reviews` | Standardized measure of player satisfaction. |
| **`days_since_release`** | `snapshot_dt - release_dt` (in days) | Measures the market age or maturity of the game. |
| **`main_genre`** | First element of the `genres` list | Represents the game's primary category. Set to 'Unknown' if the list is empty. |
| **`original_price_usd`**| `original_price_cents / 100` | Price conversion for easier analysis in USD. |
| **`is_free`** | Cast to integer (0 or 1) | Binary indicator of a free-to-play title. |

#### 4. Data Limitations and Extensions

* **Data Limitations:** The `owners_proxy` variable is based on ranges reported by SteamSpy and is an estimate, not a direct sales figure. Furthermore, the dataset represents a single snapshot in time, missing insights into sales over time or seasonal trends.
* **Extensions:** Future work could involve collecting additional data, such as detailed user tags, full-text review content , or using an updated collection schedule to enable time-series analysis of price changes and review decay.
