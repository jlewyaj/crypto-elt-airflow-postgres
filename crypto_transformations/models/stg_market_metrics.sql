{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('public_source', 'market_metrics_staging') }}
)

select
    -- Mapping the exact 5 columns found in your physical database
    coin_id::varchar(100) as coin_id,
    price_usd::numeric(20, 4) as usd_price,
    market_cap_usd::numeric(25, 2) as market_cap_usd,
    volume_24h_usd::numeric(25, 2) as daily_volume_usd,
    extracted_at::timestamp as extraction_timestamp
    
from raw_data