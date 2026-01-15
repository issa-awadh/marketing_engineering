with source as (
    select * from {{ ref('ad_spend') }}
),

cleaned as (
    select
        date,
        platform,
        -- Fix inconsistent campaign naming (e.g., 'summer_sale' vs 'Summer Sale')
        -- We make everything lowercase and replace spaces with underscores for consistency
        lower(replace(campaign_name, ' ', '_')) as campaign_name,
        cost
    from source
    -- Filter out rows where campaign_name is null (bad data)
    where campaign_name is not null
)

select * from cleaned