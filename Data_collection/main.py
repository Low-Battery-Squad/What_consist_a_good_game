import ast
from typing import Any, Tuple

from fetch_raw_data import run_from_config


def parse_tuple_input(raw: str) -> Tuple[Any, ...]:
    '''
    Parse the raw string typed by the user into a configuration tuple.
    The expected format is a Python-style tuple or list, such as
    (500, 0, 0, 0, "", 0).
    '''
    try:
        value = ast.literal_eval(raw)
    except Exception as e:
        raise ValueError(f"Could not parse input as a tuple: {e}")

    if isinstance(value, list):
        value = tuple(value)

    if not isinstance(value, tuple):
        raise ValueError("Input must be a tuple or list.")

    if len(value) not in (5, 6):
        raise ValueError("Config must have 5 or 6 elements.")

    return value


def main() -> None:
    '''
    Prompt the user for a compact configuration tuple
    , then call run_from_config to collect the data.
    '''
    print(
        "=== Steam game data collector (config mode) ===\n"
        "Config format:\n"
        "  (target_n, min_year, price_flag, sample_mode_flag, genre_string, max_candidates)\n"
        "Example:\n"
        '  (500, 0, 0, 1, "", 0)\n'
    )

    raw_cfg = input("Please enter your config tuple: ").strip()
    config = parse_tuple_input(raw_cfg)

    output_path = "Rawdata/games_filtered.json"

    print("Working... collecting data from Steam API based on your config.\n")
    run_from_config(config, output_path)

    print(f"Done. Saved filtered games to {output_path}")


if __name__ == "__main__":
    main()
