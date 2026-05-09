import requests
import polars as pl
from pathlib import Path

URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"

out_dir = Path("../../data/raw/teams")
out_dir.mkdir(parents=True, exist_ok=True)

data = requests.get(URL).json()["teams"]

rows = []

for t in data:
    rows.append({
        "team_id": t["id"],
        "name": t["name"],
        "abbreviation": t.get("abbreviation"),
        "team_code": t.get("teamCode"),
        "file_code": t.get("fileCode"),
        "league_id": t["league"]["id"],
        "division_id": t.get("division", {}).get("id"),
        "venue_id": t.get("venue", {}).get("id"),
        "active": t.get("active"),
    })

df = pl.DataFrame(rows)

df.write_parquet(
    out_dir / "teams.parquet",
    compression="zstd"
)