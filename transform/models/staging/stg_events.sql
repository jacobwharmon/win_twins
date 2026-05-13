with raw_events as (

    select *
    from {{ source('raw', 'events') }}

),

typed as (

    select
        cast(game_pk as INT) as game_pk,
        cast(start_time as TIMESTAMP) as event_start_timestamp,
        cast(end_time as TIMESTAMP) as event_end_timestamp,
        cast(pitcher_id as INT) as pitcher_id,
        cast(batter_id as INT) as batter_id,
        cast(balls as INT) as balls,
        cast(strikes as INT) as strikes,
        cast(outs as INT) as outs,
        cast(inning as INT) as inning,
        cast(half_inning as VARCHAR) as half_inning,
        cast(at_bat_index as INT) as at_bat_index,
        cast(event_type as VARCHAR) as event_type,
        cast(description as VARCHAR) as event_description,
        cast(is_scoring_play as BOOLEAN) as is_scoring_play,
        cast(rbi as INT) as rbi,
        cast(raw_json as JSON) as event_json
    from raw_events

),

final as (

    select *
    from typed

)

select * from final
