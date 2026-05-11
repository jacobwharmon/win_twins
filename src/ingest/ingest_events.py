from datetime import date
from pathlib import Path
from time import sleep

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

GAMES_PATH = "../../data/raw/games"
SEASONS = range(2020, date.today().year + 1)

out_dir = Path("../../data/raw/events")
out_dir.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# SESSION + RETRIES (IMPORTANT FIX)
# --------------------------------------------------

session = requests.Session()

retry_strategy = Retry(
    total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)

session.mount("https://", adapter)
session.mount("http://", adapter)

# --------------------------------------------------
# LOAD GAMES
# --------------------------------------------------

games_df = pl.read_parquet(f"{GAMES_PATH}/*.parquet")

# --------------------------------------------------
# PROCESS EACH SEASON
# --------------------------------------------------

for season in SEASONS:
    print(f"\nProcessing season {season}")

    season_games = games_df.filter(pl.col("season") == season).select("game_pk").unique()

    game_pks = season_games.to_series().to_list()

    print(f"{len(game_pks)} games found")

    rows = []
    failed_games = []

    # --------------------------------------------------
    # PROCESS GAMES
    # --------------------------------------------------

    for i, game_pk in enumerate(game_pks):
        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

        try:
            response = session.get(url, timeout=(10, 60))
            response.raise_for_status()

            data = response.json()

            plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])

            for play in plays:
                matchup = play.get("matchup", {})
                result = play.get("result", {})
                count = play.get("count", {})
                about = play.get("about", {})

                rows.append(
                    {
                        "game_pk": game_pk,
                        "at_bat_index": about.get("atBatIndex"),
                        "inning": about.get("inning"),
                        "half_inning": about.get("halfInning"),
                        "event_type": result.get("eventType"),
                        "event": result.get("event"),
                        "description": result.get("description"),
                        "batter_id": matchup.get("batter", {}).get("id"),
                        "pitcher_id": matchup.get("pitcher", {}).get("id"),
                        "balls": count.get("balls"),
                        "strikes": count.get("strikes"),
                        "outs": count.get("outs"),
                        "start_time": about.get("startTime"),
                        "end_time": about.get("endTime"),
                        "is_scoring_play": about.get("isScoringPlay"),
                        "rbi": result.get("rbi"),
                        # raw payload preserved
                        "raw_json": str(play),
                    }
                )

            if i % 100 == 0:
                print(f"{season}: {i}/{len(game_pks)} games")

            sleep(0.1)

        except Exception as e:
            failed_games.append(game_pk)
            print(f"Failed game {game_pk}: {e}")

    # --------------------------------------------------
    # WRITE SEASON PARQUET
    # --------------------------------------------------

    season_df = pl.DataFrame(rows)

    output_path = out_dir / f"events_{season}.parquet"

    season_df.write_parquet(output_path, compression="zstd")

    print(f"\n{season}: {season_df.shape[0]} events written")

    # --------------------------------------------------
    # WRITE FAILED GAMES LOG
    # --------------------------------------------------

    if failed_games:
        pl.DataFrame({"game_pk": failed_games}).write_csv(out_dir / f"failed_games_{season}.csv")

        print(f"{season}: {len(failed_games)} failed games logged")
