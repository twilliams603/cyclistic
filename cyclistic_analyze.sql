
/**************
**  ANALYZE  **
**************/

-- TOTAL RIDES

-- total rides overall (4,175,528)
SELECT COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`;

-- total rides by rider type
SELECT
  rider_type,
  COUNT(ride_id) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1;

-- total rides by bike type, separated by rider type
SELECT
  rider_type,
  bike_type,
  COUNT(ride_id) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY bike_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_bike_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- total rides by hour of day, separated by rider type
SELECT
  rider_type,
  PARSE_TIME ('%H', CAST(EXTRACT(HOUR FROM started_at) AS STRING)) AS start_hour,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- total rides by time of day, separated by rider type
SELECT
  rider_type,
  time_of_day,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total,
  CASE 
    WHEN time_of_day = 'Morning' THEN '1'
    WHEN time_of_day = 'Afternoon' THEN '2'
    WHEN time_of_day = 'Evening' THEN '3'
    WHEN time_of_day = 'Night' THEN '4'
    ELSE 'Unknown'
  END AS time_of_day_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 6
ORDER BY 1, 6;

-- total rides by day of week, separated by rider type
SELECT
  rider_type,
  day_of_week,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total,
  (CAST(FORMAT_DATE('%w', started_at) AS INT64) + 1) AS day_of_week_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 6
ORDER BY 1, 6;

-- total rides by month, separated by rider type
SELECT
  rider_type,
  month,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total,
  FORMAT_DATE('%m', started_at) AS month_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 6
ORDER BY 1, 6;

-- total rides by quarter, separated by rider type
SELECT
  rider_type,
  quarter,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*))
    OVER(
      PARTITION BY rider_type
      ORDER BY rider_type
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 1
  ) AS pct_of_rider_type,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) 
    OVER(),1
  ) AS pct_of_total
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- AVERAGE RIDE TIME AND STATION DISTANCE

-- average ride time and station distance by rider type
SELECT
  rider_type,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY 1;

-- average ride time and station distance by bike type, separated by rider type
SELECT
  rider_type,
  bike_type,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- average ride time and station distance by hour of day, separated by rider type
SELECT
  rider_type,
  PARSE_TIME ('%H', CAST(EXTRACT(HOUR FROM started_at) AS STRING)) AS start_hour,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- average ride time and station distance by time of day, separated by rider type
SELECT
  rider_type,
  time_of_day,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance,
  CASE 
    WHEN time_of_day = 'Morning' THEN '1'
    WHEN time_of_day = 'Afternoon' THEN '2'
    WHEN time_of_day = 'Evening' THEN '3'
    WHEN time_of_day = 'Night' THEN '4'
    ELSE 'Unknown'
  END AS time_of_day_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 5
ORDER BY 1, 5;

-- average ride time and station distance by day of week, separated by rider type
SELECT
  rider_type,
  day_of_week,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance,
  (CAST(FORMAT_DATE('%w', started_at) AS INT64) + 1) AS day_of_week_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 5
ORDER BY 1, 5;

-- average ride time and station distance by month, separated by rider type
SELECT
  rider_type,
  month,  
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance,
  FORMAT_DATE('%m', started_at) AS month_num
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2, 5
ORDER BY 1, 5;

-- average ride time and station distance by quarter, separated by rider type
SELECT
  rider_type,
  quarter,
  ROUND(AVG(ride_duration), 1) AS avg_ride_duration,
  ROUND(AVG(station_distance), 1) AS avg_station_distance
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY 1, 2;

-- STATIONS, ROUTES, AND NEIGHBORHOODS

-- 10 most popular stations (returns number of rides that started or ended at station)
SELECT
  station_name,
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 4175528,1) AS pct_of_total
FROM (
  SELECT 
    start_station_name AS station_name, 
    start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  UNION ALL
  SELECT
    end_station_name AS station_name,
    end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE start_station_name != end_station_name
  )
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- most popular stations - for Tableau
SELECT *
FROM (
  SELECT 
    start_station_name AS station_name, 
    start_neighborhood AS neighborhood,
    ROUND(start_lat, 3) AS lat,
    ROUND(start_lng, 3) AS lng,
    ride_id,
    bike_type,
    rider_type,
    started_at AS docked_at,
    time_of_day,
    day_of_week,
    day,
    month,
    year,
    quarter,
    ride_duration,
    station_distance
  FROM `cyclistic.trips_cleaned`
  UNION ALL
  SELECT
    end_station_name AS station_name,
    end_neighborhood AS neighborhood,
    ROUND(end_lat, 3) AS lat,
    ROUND(end_lng, 3) AS lng,
    ride_id,
    bike_type,
    rider_type,
    ended_at AS docked_at,
    CASE
      WHEN 0 <= EXTRACT(HOUR FROM ended_at) AND EXTRACT(HOUR FROM ended_at) < 6 THEN 'Night'
      WHEN 6 <= EXTRACT(HOUR FROM ended_at) AND EXTRACT(HOUR FROM ended_at) < 12 THEN 'Morning'
      WHEN 12 <= EXTRACT(HOUR FROM ended_at) AND EXTRACT(HOUR FROM ended_at) < 18 THEN 'Afternoon' 
      WHEN 18 <= EXTRACT(HOUR FROM ended_at) AND EXTRACT(HOUR FROM ended_at) < 24 THEN 'Evening' 
      ELSE 'Unknown'
    END AS time_of_day, 
    FORMAT_DATE('%a', ended_at) AS day_of_week,
    FORMAT_DATE('%d', ended_at) AS day,
    FORMAT_DATE('%b', ended_at) AS month,
    FORMAT_DATE('%Y', ended_at) AS year,
    CONCAT('Q',FORMAT_DATE('%Q', ended_at)) AS quarter,
    ride_duration,
    station_distance
  FROM `cyclistic.trips_cleaned`
  WHERE start_station_name != end_station_name
  )
ORDER BY station_name;

-- 10 most popular start stations
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end stations
SELECT
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular routes
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`
GROUP BY 1, 2, 3, 4
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular neighborhoods
SELECT
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 4175528,1) AS pct_of_total
FROM (
  SELECT start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  UNION ALL
  SELECT end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE start_neighborhood != end_neighborhood
  )
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular start neighborhoods
SELECT
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end neighborhoods
SELECT
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular stations for casual riders
SELECT
  station_name,
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 1562740,1) AS pct_of_total
FROM (
  SELECT 
    start_station_name AS station_name, 
    start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'casual'
  UNION ALL
  SELECT
    end_station_name AS station_name,
    end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'casual'
    AND start_station_name != end_station_name
  )
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular start stations for casual riders
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'casual'
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end stations for casual riders
SELECT
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'casual'
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular routes for casual riders
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'casual'
GROUP BY 1, 2, 3, 4
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular neighborhoods for casual riders
SELECT
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 1562740,1) AS pct_of_total
FROM (
  SELECT start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'casual'
  UNION ALL
  SELECT end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'casual'
    AND start_neighborhood != end_neighborhood
  )
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular start neighborhoods for casual riders
SELECT
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'casual'
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end neighborhoods for casual riders
SELECT
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'casual'
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular stations for members
SELECT
  station_name,
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 2612788,1) AS pct_of_total
FROM (
  SELECT 
    start_station_name AS station_name, 
    start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'member'
  UNION ALL
  SELECT
    end_station_name AS station_name,
    end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'member'
    AND start_station_name != end_station_name
  )
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular start stations for members
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'member'
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end stations for members
SELECT
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'member'
GROUP BY 1, 2
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular routes for members
SELECT
  start_station_name AS start_station,
  start_neighborhood,
  end_station_name AS end_station,
  end_neighborhood,
  COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'member'
GROUP BY 1, 2, 3, 4
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular neighborhoods for members
SELECT
  neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / 2612788,1) AS pct_of_total
FROM (
  SELECT start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'member'
  UNION ALL
  SELECT end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE rider_type = 'member'
    AND start_neighborhood != end_neighborhood
  )
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular start neighborhoods for members
SELECT
  start_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'member'
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- 10 most popular end neighborhoods for members
SELECT
  end_neighborhood,
  COUNT(*) AS rides,
  ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),1) AS pct_of_total
FROM `cyclistic.trips_cleaned`
WHERE rider_type = 'member'
GROUP BY 1
ORDER BY rides DESC
LIMIT 10;

-- least popular stations (returns 62 stations with only 1 ride that started or ended there)
SELECT
  station_name,
  COUNT(*) AS rides
FROM (
  SELECT 
    start_station_name AS station_name, 
    start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  UNION ALL
  SELECT
    end_station_name AS station_name,
    end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE start_station_name != end_station_name
  )
GROUP BY 1
ORDER BY rides;

-- least popular start stations (112 stations have only 1 ride)
SELECT
  start_station_name AS start_station,
  COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY rides;

-- least popular end stations (105 stations have only 1 ride)
SELECT
  end_station_name AS end_station,
  COUNT(*) AS rides
FROM `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY rides;

-- 10 least popular neighborhoods
SELECT
  neighborhood,
  COUNT(*) AS rides,
FROM (
  SELECT start_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  UNION ALL
  SELECT end_neighborhood AS neighborhood
  FROM `cyclistic.trips_cleaned`
  WHERE start_neighborhood != end_neighborhood
  )
GROUP BY 1
ORDER BY rides
LIMIT 10;

-- MISC

-- total cumulative ride time by rider type (hours)
SELECT
  rider_type,
  ROUND(SUM(ride_duration) / 60, 1) AS total_ride_time_hrs -- in hours
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY 1;

-- total cumulative ride time by rider type (years)
SELECT
  rider_type,
  ROUND(SUM(ride_duration) / 60 / 24 / 365, 1) AS total_ride_time_years -- in years
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY 1;

-- maximum ride time and station distance by rider type (confirms that data cleaning limited data to ride times of less than 24 hours)
SELECT
  rider_type,
  ROUND(MAX(ride_duration) / 60, 1) AS max_ride_duration_hrs, -- in hours
  ROUND(MAX(station_distance), 1) AS max_station_distance, -- in meters
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY 1;

-- identify rides with longest duration (returns 15 rides that were 23.9 hours)
SELECT
  ride_id,
  rider_type,
  start_station_name AS start_station,
  end_station_name AS end_station,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  ride_duration,
  station_distance
FROM
  `cyclistic.trips_cleaned`
WHERE ROUND(ride_duration / 60, 1) = 23.9;

-- identify rides with longest duration, querying original data, which has no limits on ride duration. (returns 5292 rides of more than 24 hrs. most have missing lat/lng)
SELECT
  ride_id,
  member_casual,
  start_station_name AS start_station,
  end_station_name AS end_station,
  started_at,
  ended_at,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  ROUND((TIMESTAMP_DIFF (ended_at, started_at, minute)) / 60, 1) AS ride_duration_hrs, -- ride duration in hours
  ROUND((TIMESTAMP_DIFF (ended_at, started_at, minute)) / 60 / 24, 1) AS ride_duration_days, -- ride duration in days
  CAST( ST_DISTANCE(
    ST_GEOGPOINT(ROUND(start_lng, 4), ROUND(start_lat, 4)), 
    ST_GEOGPOINT(ROUND(end_lng, 4), ROUND(end_lat, 4))
    ) AS INT64) AS station_distance, -- distance from start station to end station in meters
FROM
  `cyclistic.trips`
WHERE TIMESTAMP_DIFF (ended_at, started_at, hour) >= 24
ORDER BY ride_duration_days DESC;

-- identify ride with longest station distance, querying cleaned data (returns ride from 71st St & South Shore Dr all the way to Northwestern U)
SELECT
  ride_id,
  rider_type,
  start_station_name AS start_station,
  end_station_name AS end_station,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  ride_duration,
  station_distance
FROM
  `cyclistic.trips_cleaned`
WHERE ROUND(station_distance, 1) = 33081.0;

-- minimum ride time and station distance by rider type (2 minutes and 0 meters)
SELECT
  rider_type,
  ROUND(MIN(ride_duration), 1) AS min_ride_duration, -- in minutes
  ROUND(MIN(station_distance), 1) AS min_station_distance -- in meters
FROM
  `cyclistic.trips_cleaned`
GROUP BY 1
ORDER BY 1;

