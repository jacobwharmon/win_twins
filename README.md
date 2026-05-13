## Doncha know the Twins?
I love the Minnesota Twins.  
  
They are a rag-tag bunch of old and new talent, they are fun to watch, and yet they are **consistently undersold** in power rankings, news coverage, and MVP bids.  
  
They deserve better.  
  
## What is this project?
Courtesy of the [MLB Stats API](https://statsapi.mlb.com/api/v1) I construct an end-to-end MLB data project to ingest, model, and viz Minnesota's **strengths** when they're up and **silver linings** when they're down.

## High-level Project Architecture
- Ingest MLB event-, game-, player-, and team-level data from 2020-current via MLB Stats API
- Store raw ingest data as parquet
- Model a star schema with the central fact table at the player-game grain using dbt and duckdb for ELT
- Visualize 
- All of the above using Github Actions free tier for CI/CD and daily execution