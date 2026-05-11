from datetime import datetime
from pathlib import Path
from time import sleep

import polars as pl
import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"

out_dir = Path("../../data/raw/players")
out_dir.mkdir(parents=True, exist_ok=True)

START_SEASON = 2020
END_SEASON = datetime.now().year

teams = requests.get(TEAMS_URL).json()["teams"]

for season in range(START_SEASON, END_SEASON + 1):
    print(f"\n=== Season {season} ===")

    rows = []

    for team in teams:
        team_id = team["id"]

        roster_url = (
            f"https://statsapi.mlb.com/api/v1/teams/"
            f"{team_id}/roster"
            f"?season={season}&rosterType=fullSeason"
        )

        try:
            resp = requests.get(roster_url)
            resp.raise_for_status()

            roster = resp.json().get("roster", [])

            for p in roster:
                person = p["person"]

                rows.append(
                    {
                        "player_id": person["id"],
                        "full_name": person["fullName"],
                        "team_id": team_id,
                        "season": season,
                        "position": p.get("position", {}).get("abbreviation"),
                        "status": p.get("status", {}).get("description"),
                    }
                )

            print(f"Loaded {team['name']} ({season})")

        except Exception as e:
            print(f"Failed for team {team_id} season {season}: {e}")

        sleep(0.25)

    df = pl.DataFrame(rows).unique(subset=["player_id"]).sort("player_id")

    out_path = out_dir / f"players_{season}.parquet"

    df.write_parquet(out_path, compression="zstd")

    print(f"Wrote {out_path} ({df.height:,} players)")
