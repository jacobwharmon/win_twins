with raw_games as (

    select * from {{ source('raw', 'games')}}

)

select * from raw_games