with raw_games as (

    select * from {{ source('raw', 'games') }}

),

typed as (

    select
        cast(game_pk as INT) as game_pk,
        cast(game_date as TIMESTAMP) as game_start_timestamp,
        cast(season as INT) as season,
        cast(game_type as VARCHAR) as game_type,
        cast(home_team_id as INT) as home_team_id,
        cast(away_team_id as INT) as away_team_id,
        cast(home_score as INT) as home_score_final,
        cast(away_score as INT) as away_score_final,
        cast(winning_team_id as INT) as winning_team_id,
        cast(losing_team_id as INT) as losing_team_id,
        cast(status as VARCHAR) as game_status,
        cast(scheduled_innings as INT) as scheduled_innings,
        cast(series_game_number as INT) as series_game_number,
        cast(games_in_series as INT) as games_in_series,
        cast(venue_id as INT) as venue_id,
        cast(venue_name as VARCHAR) as venue_name
    from raw_games

),

final as (

    select * from typed

)

select * from final
