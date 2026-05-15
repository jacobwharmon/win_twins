with raw_players as (

    select * from {{ source('raw', 'players') }}

),

typed as (

    select
        cast(player_id as INT) as player_id,
        cast(full_name as VARCHAR) as player_full_name
    from raw_players

),

deduped as (

    select distinct
        player_id,
        player_full_name
    from typed

),

final as (

    select * from deduped

)

select * from final
