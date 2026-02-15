# dbt_zoomcamp

# HOMEWORK

# Question 4

SELECT pickup_zone, sum(revenue_month) as total_revenue_year, 
RANK() OVER (ORDER BY SUM(revenue_month) DESC) as rank FROM `kestra-course-485912.dbt_prod.fct_monthly_zone_revenue` where service_type = 'Green' and year = '2020' group by pickup_zone order by rank

# Question 5

SELECT sum(trip_count) as total_october_trips, 
 FROM `kestra-course-485912.dbt_prod.fct_monthly_zone_revenue` where service_type = 'Green' and month = '10' and year = '2019'

 # Question 6

 The staging fhv model is located in staging folder

 SELECT count(*) FROM `kestra-course-485912.dbt_prod.stg_fhv_tripdata` 