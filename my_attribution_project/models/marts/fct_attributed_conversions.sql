with sessions as (
    select * from {{ ref('int_user_sessions') }}
),

-- We only care about sessions that led to a conversion eventually.
-- But for Multi-Touch Attribution, we need ALL history for those converting users.
conversions as (
    select distinct user_id 
    from sessions 
    where interaction = 'conversion'
)

select 
    s.session_id,
    s.user_id,
    s.timestamp,
    s.source,
    s.medium,
    s.campaign_name,
    s.interaction,
    s.conversion_value
from sessions s
-- Filter to keep only users who actually converted (so we can analyze what led to it)
inner join conversions c on s.user_id = c.user_id
order by s.user_id, s.timestamp