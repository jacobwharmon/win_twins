with raw_events as (

    select * from {{ source('raw', 'events')}}

)

select * from raw_events