with user_journeys as (
    select * from {{ ref('stg_user_journeys') }}
),

-- 1. Calculate the time difference between the current click and the previous click
time_calculations as (
    select
        *,
        -- LAG looks at the "previous" row for this user
        lag(timestamp) over (partition by user_id order by timestamp) as previous_timestamp,
        -- Calculate minutes since last activity
        timestamp_diff(
            timestamp, 
            lag(timestamp) over (partition by user_id order by timestamp), 
            MINUTE
        ) as minutes_since_last_click
    from user_journeys
),

-- 2. Flag where a new session starts (if gap > 30 mins OR it's the first click)
session_flags as (
    select
        *,
        case
            when minutes_since_last_click is null then 1 -- First click ever
            when minutes_since_last_click > 30 then 1    -- Gap > 30 mins
            else 0 
        end as is_new_session
    from time_calculations
),

-- 3. Assign a unique Session ID using a running sum
-- Every time 'is_new_session' is 1, the sum increases, creating a new ID
session_ids as (
    select
        *,
        sum(is_new_session) over (partition by user_id order by timestamp rows between unbounded preceding and current row) as session_sequence
    from session_flags
)

-- Final Output: Clean table with a unique Session ID for every row
select
    -- Create a truly unique ID: UserID + Sequence (e.g., "505-1", "505-2")
    concat(cast(user_id as string), '-', cast(session_sequence as string)) as session_id,
    user_id,
    timestamp,
    source,
    medium,
    campaign_name,
    interaction,
    conversion_value
from session_ids