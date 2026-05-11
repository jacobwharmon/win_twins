from datetime import date
from pathlib import Path

import polars as pl
import requests

SEASONS = range(2020, date.today().year + 1)

out_dir = Path("../../data/raw/games")
out_dir.mkdir(parents=True, exist_ok=True)

for season in SEASONS:
    print(f"Loading season {season}...")

    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}"

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()["dates"]

    rows = []

    for d in data:
        for g in d["games"]:
            home = g.get("teams", {}).get("home", {})
            away = g.get("teams", {}).get("away", {})

            rows.append(
                {
                    "game_pk": g.get("gamePk"),
                    "game_date": g.get("gameDate"),
                    "season": int(g.get("season")),
                    "game_type": g.get("gameType"),
                    "home_team_id": (home.get("team", {}).get("id")),
                    "away_team_id": (away.get("team", {}).get("id")),
                    "home_score": home.get("score"),
                    "away_score": away.get("score"),
                    "venue_id": (g.get("venue", {}).get("id")),
                    "venue_name": (g.get("venue", {}).get("name")),
                    "status": (g.get("status", {}).get("detailedState")),
                    "scheduled_innings": g.get("scheduledInnings"),
                    "series_game_number": (g.get("seriesGameNumber")),
                    "games_in_series": (g.get("gamesInSeries")),
                    "winning_team_id": (
                        home.get("team", {}).get("id")
                        if home.get("isWinner") is True
                        else away.get("team", {}).get("id")
                        if away.get("isWinner") is True
                        else None
                    ),
                    "losing_team_id": (
                        away.get("team", {}).get("id")
                        if home.get("isWinner") is True
                        else home.get("team", {}).get("id")
                        if away.get("isWinner") is True
                        else None
                    ),
                    "winning_pitcher_id": (g.get("decisions", {}).get("winner", {}).get("id")),
                    "losing_pitcher_id": (g.get("decisions", {}).get("loser", {}).get("id")),
                    "save_pitcher_id": (g.get("decisions", {}).get("save", {}).get("id")),
                }
            )

    df = pl.DataFrame(rows)

    output_path = out_dir / f"games_{season}.parquet"

    df.write_parquet(output_path, compression="zstd")

    print(f"{season}: {len(rows)} games written")
