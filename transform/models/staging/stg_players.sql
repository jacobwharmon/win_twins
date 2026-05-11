with raw_players as (

    select * from {{ source('raw', 'players')}}

)

select * from raw_players