with raw_data as (
    select * from {{ ref('user_journeys') }}
),

cleaned as (
    select
        user_id,
        timestamp,
        -- Now there is no ambiguity
        lower(source) as source,
        medium,
        -- Standardize campaign names
        lower(replace(campaign, ' ', '_')) as campaign_name,
        interaction,
        conversion_value
    from raw_data
)

select * from cleaned