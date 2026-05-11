with raw_teams as (

    select * from {{ source('raw', 'teams') }}

)

select * from raw_teams
