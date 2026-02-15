
{{
  config(
    materialized='table',
    unique_key=['year','month', 'pickup_location_id', 'service_type'],
    incremental_strategy='merge'
  )
}}

WITH trips AS (
  SELECT
    FORMAT_TIMESTAMP('%Y', pickup_datetime) AS year,
    FORMAT_TIMESTAMP('%m', pickup_datetime) AS month,
    pickup_location_id,
    service_type,
    fare_amount,
    total_amount,
    trip_distance
  FROM {{ ref('fct_trips') }}
)

-- Monthly revenue by pickup zone
-- This aggregates the fact_trips table by month and pickup zone, calculating total revenue
-- Materialized incrementally to efficiently handle growing dataset

select
    -- Trip identifiers
  trips.year,
  trips.month,
  trips.pickup_location_id AS zone_id,
  zones.zone as pickup_zone,
  zones.borough,
  trips.service_type,
  
  -- Revenue metrics
  SUM(trips.total_amount) AS revenue_month,
  SUM(trips.fare_amount) AS fare_amount_month,
  
  -- Volume metrics
  COUNT(*) AS trip_count,
  
  -- Average metrics
  AVG(trips.fare_amount) AS avg_fare_amount,
  AVG(trips.total_amount) AS avg_total_amount,
  AVG(trips.trip_distance) AS avg_trip_distance

from trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
left join {{ ref('dim_zones') }} as zones
    on trips.pickup_location_id = zones.location_id
GROUP BY 1, 2, 3, 4, 5, 6

