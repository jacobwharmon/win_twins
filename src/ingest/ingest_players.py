import requests
import polars as pl
from pathlib import Path
from time import sleep

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"

out_dir = Path("../../data/raw/players")
out_dir.mkdir(parents=True, exist_ok=True)

teams = requests.get(TEAMS_URL).json()["teams"]

rows = []

for team in teams:
    team_id = team["id"]

    roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

    try:
        roster = requests.get(roster_url).json()["roster"]

        for p in roster:
            person = p["person"]

            rows.append({
                "player_id": person["id"],
                "full_name": person["fullName"],
                "team_id": team_id,
                "position": p.get("position", {}).get("abbreviation"),
                "status": p.get("status", {}).get("description"),
            })

        print(f"Loaded roster for {team['name']}")

    except Exception as e:
        print(f"Failed for team {team_id}: {e}")

    sleep(0.25)

df = (
    pl.DataFrame(rows)
    .unique(subset=["player_id"])
)

df.write_parquet(
    out_dir / "players.parquet",
    compression="zstd"
)