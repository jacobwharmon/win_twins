import requests
import polars as pl
from pathlib import Path
from datetime import date

START_DATE = "2020-01-01"
END_DATE = str(date.today())

URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
    f"?sportId=1&startDate={START_DATE}&endDate={END_DATE}"
)

out_dir = Path("../../data/raw/games")
out_dir.mkdir(parents=True, exist_ok=True)

data = requests.get(URL).json()["dates"]

rows = []

for d in data:
    for g in d["games"]:

        rows.append({
            "game_pk": g["gamePk"],
            "game_date": g["gameDate"],
            "season": g["season"],

            "home_team_id": (
                g["teams"]["home"]["team"]["id"]
            ),

            "away_team_id": (
                g["teams"]["away"]["team"]["id"]
            ),

            "venue_id": g.get("venue", {}).get("id"),

            "status": (
                g.get("status", {})
                 .get("detailedState")
            ),
        })

df = pl.DataFrame(rows)

df.write_parquet(
    out_dir / "games.parquet",
    compression="zstd"
)