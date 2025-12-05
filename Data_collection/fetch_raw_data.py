import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import os
import random
import re

import requests
from dotenv import load_dotenv

load_dotenv()

APPLIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
STEAM_APPREVIEWS_URL = "https://store.steampowered.com/appreviews/"
STEAMSPY_URL = "https://steamspy.com/api.php"


def get_app_list(max_results: int = 1000) -> List[Dict[str, Any]]:
    '''
    Call the Steam IStoreService/GetAppList endpoint and return
    a list of app records (each record contains at least appid and name).
    The result is limited by max_results.
    '''
    api_key = os.environ["STEAM_API_KEY"]

    params = {
        "key": api_key,
        "include_games": True,
        "include_dlc": False,
        "include_software": False,
        "include_videos": False,
        "include_hardware": False,
        "max_results": max_results,
    }

    resp = requests.get(APPLIST_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    apps = data.get("response", {}).get("apps", [])
    return apps


def get_sample_app_ids(max_games: int = 300) -> List[int]:
    '''
    Use get_app_list to obtain a batch of apps and extract appid values.
    Return at most max_games appids as the sample frame for later calls.
    '''
    apps = get_app_list(max_results=max_games)
    app_ids = [app["appid"] for app in apps if app.get("appid")]
    return app_ids


def fetch_app_details(app_id: int) -> Optional[Dict[str, Any]]:
    '''
    Fetch detailed information for a single app from the Storefront
    appdetails endpoint. Only return data if the type is "game".
    Otherwise return None.
    '''
    params = {"appids": app_id, "cc": "us", "l": "en"}
    resp = requests.get(STEAM_APPDETAILS_URL, params=params, timeout=20)
    if resp.status_code != 200:
        return None

    raw = resp.json()
    entry = raw.get(str(app_id))
    if not entry or not entry.get("success"):
        return None

    data = entry.get("data", {})
    if data.get("type") != "game":
        return None

    return data


def fetch_review_summary(app_id: int) -> Optional[Dict[str, Any]]:
    '''
    Fetch aggregated review statistics for a single app from the
    /appreviews endpoint. Return the query_summary block, which
    contains total_reviews and total_positive, or None on failure.
    '''
    params = {
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "num_per_page": 0,
    }
    resp = requests.get(f"{STEAM_APPREVIEWS_URL}{app_id}", params=params, timeout=20)
    if resp.status_code != 200:
        return None

    data = resp.json()
    return data.get("query_summary")


def fetch_owners_proxy(app_id: int) -> Optional[int]:
    '''
    Query the SteamSpy appdetails API for a single app and use the
    reported owners range as a proxy for sales. The function returns
    the midpoint of the owners interval as an integer, or None if the
    value is not available or cannot be parsed.
    '''
    params = {"request": "appdetails", "appid": app_id}
    try:
        resp = requests.get(STEAMSPY_URL, params=params, timeout=20)
        if resp.status_code != 200:
            return None

        data = resp.json()
        owners_str = data.get("owners")
        if not owners_str:
            return None

        cleaned = owners_str.replace(",", "").replace(" ", "")
        parts = cleaned.split("..")
        if len(parts) != 2:
            return None

        low = int(parts[0])
        high = int(parts[1])
        return (low + high) // 2

    except (ValueError, TypeError) as e:
        print(f"SteamSpy parse error for app {app_id}: {e}")
        return None
    except Exception as e:
        print(f"SteamSpy error for app {app_id}: {e}")
        return None


def fetch_and_save_raw_data(output_path: str, max_games: int = 300) -> None:
    '''
    Orchestrate the full data collection pipeline:
    1) sample a set of appids,
    2) fetch appdetails, review summaries, and owners proxy for each app,
    3) assemble the fields into a list of dictionaries,
    4) save the resulting list as a JSON file at output_path.
    '''
    snapshot_time = datetime.utcnow().isoformat() + "Z"

    app_ids = get_sample_app_ids(max_games=max_games)
    results: List[Dict[str, Any]] = []

    for idx, app_id in enumerate(app_ids, start=1):
        try:
            details = fetch_app_details(app_id)
            if not details:
                continue

            reviews = fetch_review_summary(app_id)
            total_reviews = None
            positive_reviews = None
            if reviews:
                total_reviews = reviews.get("total_reviews")
                positive_reviews = reviews.get("total_positive")

            owners_proxy = fetch_owners_proxy(app_id)

            price = details.get("price_overview") or {}
            original_price = price.get("initial")
            current_price = price.get("final")

            genres = details.get("genres") or []
            genre_list = [g.get("description") for g in genres if g.get("description")]

            row = {
                "app_id": app_id,
                "name": details.get("name"),
                "release_date": details.get("release_date", {}).get("date"),
                "original_price_cents": original_price,
                "current_price_cents": current_price,
                "is_free": details.get("is_free"),
                "genres": genre_list,
                "total_reviews": total_reviews,
                "positive_reviews": positive_reviews,
                "owners_proxy": owners_proxy,
                "snapshot_time": snapshot_time,
                "raw_appdetails": details,
                "raw_review_summary": reviews,
            }

            results.append(row)

            if idx % 50 == 0:
                print(f"Fetched {idx} apps...")
            time.sleep(0.3)

        except Exception as e:
            print(f"Error on app_id={app_id}: {e}")
            continue

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(results)} records to {output_path}")


def fetch_filtered_games(
    output_path: str,
    target_n: int = 500,
    min_year: Optional[int] = None,
    target_main_genre: Optional[str] = None,
    free_only: Optional[bool] = None,
    sample_mode: str = "random",
    max_candidates: int = 5000,
) -> None:
    '''
    Fetch a filtered sample of games based on user-specified conditions
    and sampling mode.

    Conditions:
    - min_year: only keep games whose release year is at least min_year.
    - target_main_genre: if provided, only keep games whose genres match
      the target. For "Indie", a game is kept if "Indie" appears in its
      genre list; for other genres, the first genre is used as main_genre.
    - free_only: if True, keep only free-to-play games; if False, keep
      only non-free games; if None, ignore this condition.

    Sampling modes:
    - "random": randomly select up to target_n games from all candidates
      that satisfy the conditions. The function stops early once the
      target size is reached.
    - "top": collect games that satisfy the conditions, then rank them
      by popularity and take the top target_n. Popularity is measured
      primarily by owners_proxy, with total_reviews as a secondary key.

    max_candidates semantics:
    - -1: no soft upper bound; use a large internal upper bound for the
      app list and scan all returned app ids until target_n games are
      collected or the list is exhausted.
    - > 0: soft limit on how many app ids are examined.
    - other values should be handled before this function is called.

    The final selected games are written as a JSON list of records to
    output_path, using the same schema as fetch_and_save_raw_data.
    '''
    snapshot_time = datetime.utcnow().isoformat() + "Z"

    if max_candidates == -1:
        app_ids = get_sample_app_ids(max_games=50000)
    else:
        app_ids = get_sample_app_ids(max_games=max_candidates)

    if sample_mode == "random":
        random.shuffle(app_ids)

    candidates: List[Dict[str, Any]] = []

    for idx, app_id in enumerate(app_ids, start=1):
        try:
            details = fetch_app_details(app_id)
            if not details:
                continue

            release_info = details.get("release_date") or {}
            release_str = release_info.get("date")
            release_year: Optional[int] = None
            if release_str:
                match = re.search(r"(\d{4})", release_str)
                if match:
                    try:
                        release_year = int(match.group(1))
                    except ValueError:
                        release_year = None

            if min_year is not None:
                if release_year is None or release_year < min_year:
                    continue

            genres = details.get("genres") or []
            genre_list = [g.get("description") for g in genres if g.get("description")]

            if target_main_genre is not None:
                if target_main_genre.lower() == "indie":
                    if "Indie" not in genre_list:
                        continue
                else:
                    main_genre = genre_list[0] if genre_list else None
                    if main_genre != target_main_genre:
                        continue

            is_free = details.get("is_free")

            if free_only is True and not is_free:
                continue
            if free_only is False and is_free:
                continue

            reviews = fetch_review_summary(app_id)
            total_reviews: Optional[int] = None
            positive_reviews: Optional[int] = None
            if reviews:
                total_reviews = reviews.get("total_reviews")
                positive_reviews = reviews.get("total_positive")

            owners_proxy = fetch_owners_proxy(app_id)

            price = details.get("price_overview") or {}
            original_price = price.get("initial")
            current_price = price.get("final")

            row = {
                "app_id": app_id,
                "name": details.get("name"),
                "release_date": release_info.get("date"),
                "original_price_cents": original_price,
                "current_price_cents": current_price,
                "is_free": is_free,
                "genres": genre_list,
                "total_reviews": total_reviews,
                "positive_reviews": positive_reviews,
                "owners_proxy": owners_proxy,
                "snapshot_time": snapshot_time,
                "raw_appdetails": details,
                "raw_review_summary": reviews,
            }

            candidates.append(row)

            if sample_mode == "random" and len(candidates) >= target_n:
                break

        except Exception as e:
            print(f"Error on app_id={app_id}: {e}")
            continue

    if not candidates:
        print("No games matched the given filters. Nothing will be saved.")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        return

    if sample_mode == "top":
        def popularity_key(r: Dict[str, Any]) -> Any:
            owners = r.get("owners_proxy") or 0
            total = r.get("total_reviews") or 0
            return (owners, total)

        candidates = sorted(candidates, key=popularity_key, reverse=True)

    if len(candidates) > target_n:
        if sample_mode == "random":
            random.shuffle(candidates)
        candidates = candidates[:target_n]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(candidates)} filtered games to {output_path}")


def parse_filter_config(
    config: Tuple[Any, ...],
    default_target_n: int = 500,
) -> Dict[str, Any]:
    '''
    Parse a compact configuration tuple into keyword arguments for
    fetch_filtered_games.

    Expected format of config:
    (target_n, min_year, price_flag, sample_mode_flag, genre_string, max_candidates)

    - target_n: desired sample size. If it is 0, empty or None, the
      default_target_n value is used instead.
    - min_year: integer year; 0 or None means no lower bound.
    - price_flag: 0 = no restriction; 1 = free games only;
      2 = paid games only.
    - sample_mode_flag: 0 = "random"; 1 = "top".
    - genre_string: target main genre; an empty string or None means
      no genre restriction.
    - max_candidates semantics:
      * -1: no soft upper bound, scan as many app ids as the app list
        endpoint returns (up to an internal cap) until target_n games
        are collected or the list is exhausted.
      * 0 / empty / None: use an automatic soft upper bound based on
        target_n and the sampling mode.
      * > 0: user-specified soft upper bound.

    The function returns a dictionary containing target_n, min_year,
    target_main_genre, free_only, sample_mode and max_candidates, which
    can be unpacked directly into fetch_filtered_games.
    '''
    if len(config) == 5:
        raw_target_n, raw_min_year, price_flag, sample_flag, raw_genre = config
        raw_max_candidates = None
    elif len(config) == 6:
        raw_target_n, raw_min_year, price_flag, sample_flag, raw_genre, raw_max_candidates = config
    else:
        raise ValueError("config must have 5 or 6 elements")

    if raw_target_n in (None, 0, "", "0"):
        target_n = default_target_n
    else:
        try:
            target_n = int(raw_target_n)
        except ValueError:
            raise ValueError("target_n must be an integer or 0")
        if target_n <= 0:
            target_n = default_target_n

    if raw_min_year in (None, 0, ""):
        min_year: Optional[int] = None
    else:
        try:
            min_year = int(raw_min_year)
        except ValueError:
            raise ValueError("min_year must be an integer or 0")

    if price_flag == 0:
        free_only: Optional[bool] = None
    elif price_flag == 1:
        free_only = True
    elif price_flag == 2:
        free_only = False
    else:
        raise ValueError("price_flag must be 0 (no), 1 (free only), or 2 (paid only)")

    if sample_flag == 0:
        sample_mode = "random"
    elif sample_flag == 1:
        sample_mode = "top"
    else:
        raise ValueError("sample_mode_flag must be 0 (random) or 1 (top)")

    if raw_genre is None:
        genre_str = ""
    else:
        genre_str = str(raw_genre).strip()

    target_main_genre: Optional[str] = genre_str or None

    if raw_max_candidates in (None, "", "0", 0):
        if sample_mode == "top":
            max_candidates = max(target_n * 5, 2000)
        else:
            max_candidates = target_n * 2
    else:
        try:
            max_candidates_parsed = int(raw_max_candidates)
        except ValueError:
            raise ValueError("max_candidates must be an integer or 0/-1")
        if max_candidates_parsed == -1:
            max_candidates = -1
        elif max_candidates_parsed <= 0:
            max_candidates = max(target_n * 10, 2000)
        else:
            max_candidates = max_candidates_parsed

    params: Dict[str, Any] = {
        "target_n": target_n,
        "min_year": min_year,
        "target_main_genre": target_main_genre,
        "free_only": free_only,
        "sample_mode": sample_mode,
        "max_candidates": max_candidates,
    }
    return params


def run_from_config(
    config: Tuple[Any, ...],
    output_path: str,
) -> None:
    '''
    Convenience wrapper that takes a compact configuration tuple and an
    output path, then calls fetch_filtered_games with the corresponding
    parameters parsed from the tuple.
    '''
    params = parse_filter_config(config)
    fetch_filtered_games(output_path=output_path, **params)
