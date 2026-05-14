with raw_teams as (

    select * from {{ source('raw', 'teams') }}

),

typed as (

    select
        cast(team_id as INT) as team_id,
        cast(name as VARCHAR) as team_name,
        cast(abbreviation as VARCHAR) as team_abbreviation,
        cast(league_id as INT) as team_league_id,
        cast(division_id as INT) as team_division_id,
        cast(active as BOOLEAN) as team_active_flag
    from raw_teams

),

final as (

    select * from typed

)

select * from final
