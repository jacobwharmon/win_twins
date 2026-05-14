with raw_players as (

    select * from {{ source('raw', 'players') }}

),

typed as (

    select
        cast(player_id as INT) as player_id,
        cast(full_name as VARCHAR) as player_full_name,
        cast(position as VARCHAR) as player_position
    from raw_players

),

final as (

    select * from typed

)

select * from final
