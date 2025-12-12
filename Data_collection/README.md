
This module fetches raw Steam game data and saves it as a JSON file for later cleaning (B), database loading (C), and analysis (D).

## 1. Data sources & reliability

We use three public data sources:

1. **Steam Web API – GetAppList**  
   - URL: https://api.steampowered.com/IStoreService/GetAppList/v1/  
   - Official app list from Valve, used only to get a large pool of `appid`s.

2. **Steam Storefront – appdetails & appreviews**  
   - URLs:
     - https://store.steampowered.com/api/appdetails
     - https://store.steampowered.com/appreviews/<APPID>?json=1  
   - Provide official game metadata and aggregated review stats (`total_reviews`, `total_positive`).

3. **SteamSpy – appdetails**  
   - URL: https://steamspy.com/api.php?request=appdetails&appid=<APPID>  
   - Third-party estimate of owners, returned as a range (e.g. "200,000 .. 500,000").  
     We take the midpoint as `owners_proxy`, so this is a **coarse sales proxy**, not an exact value.

Summary: basic game info and reviews come from official Steam endpoints.  
`owners_proxy` from SteamSpy is approximate and should be interpreted as **popularity tiers**, not exact sales.

## 2. Preparation

### Steam Web API key

We use the Steam Web API only to obtain the app list. You must:

1. Request a free Steam Web API key from Valve.  
2. Create a `.env` file in the **project root** (`What_consist_a_good_game/.env`) and add:

    STEAM_API_KEY=your_steam_api_key_here

SteamSpy does **not** require an API key.

### Folder layout

This module expects (relative to project root):

    Data_collection/
      Rawdata/
        games_filtered.json   # output of this step
      fetch_raw_data.py
      main.py

The script writes the final JSON file to:

    Data_collection/Rawdata/games_filtered.json

---

## 3. How to run & config tuple

From the project root:

    cd Data_collection
    python main.py

The script prints a short help message and then asks you to input a **config tuple**:

    (target_n, min_year, price_flag, sample_mode_flag, genre_string, max_candidates)

### Meaning of each parameter

1. **target_n**  
   Number of games you want in the final dataset.  
   Example: `500`, `800`, `2000`.  
   If `0` / empty / None, a default (500) is used.

2. **min_year**  
   Minimum release year filter.  
   Example: `2020` → keep only games released in 2020 or later.  
   `0` / empty / None → no year filter.

3. **price_flag** (free vs paid)

   - `0` – no restriction (free + paid)  
   - `1` – free games only (`is_free = True`)  
   - `2` – paid games only (`is_free = False`)

4. **sample_mode_flag** (how to choose from candidates)

   - `0` – `"random"`  
     - Shuffle candidates and stop as soon as we collect `target_n` games.
   - `1` – `"top"`  
     - Evaluate all candidates, rank by popularity, and keep the **top `target_n`**.

   Popularity is defined as:
   - primary key: `owners_proxy` (higher = more popular)  
   - secondary key: `total_reviews` (break ties)

5. **genre_string** (genre filter, optional)

   - Example: `"Action"`, `"RPG"`, `"Indie"`.  
   - `""` / None → no genre filter.  
   - Special case `"Indie"`: keep a game if `"Indie"` appears anywhere in its genre list.  
   - Other genres: the first genre from Steam is treated as `main_genre` and must equal `genre_string`.

6. **max_candidates** (how many app IDs we are willing to inspect)

   - `0` / empty / None – automatic soft limit (recommended):  
     - random mode: `max_candidates = target_n * 2`  
     - top mode: `max_candidates = max(target_n * 5, 2000)`
   - positive integer – manual soft limit, e.g. `4000` = inspect at most 4000 apps.  
   - `-1` – no soft limit (internally uses a large cap, can be very slow for large `target_n`).

After you input the tuple, the script prints:

    Working... collecting data from Steam API based on your config.

and then calls Steam / SteamSpy according to these rules until the target sample is collected or `max_candidates` is reached.

### Example configs

- 500 random games, no filters

      (500, 0, 0, 0, "", 0)


- Top 200 Indie games released since 2020, searching up to 2000 games

      (200, 2020, 0, 1, "Indie", 2000)


---

## 4. Output format

The output of this step is:

    Data_collection/Rawdata/games_filtered.json

The file is a JSON *list*. Each element corresponds to one game and contains:

- `app_id`  
- `name`  
- `release_date` (string from Steam)  
- `original_price_cents`, `current_price_cents`  
- `is_free`  
- `genres` (list of genre names)  
- `total_reviews`, `positive_reviews`  
- `owners_proxy` (SteamSpy owners midpoint)  
- `snapshot_time` (UTC timestamp when data was fetched)  
- `raw_appdetails` (full JSON from `/api/appdetails`)  
- `raw_review_summary` (full `query_summary` block from `/appreviews`)

This JSON file is the input for:

- Data_cleaning: build a clean CSV for analysis.  
- Data_load: import the cleaned CSV into PostgreSQL.
