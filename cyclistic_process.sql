
/**************
**  PREPARE  **
**************/

-- drop table if existing
DROP TABLE IF EXISTS `cyclistic.trips`;
DROP TABLE IF EXISTS `cyclistic.trips_valid_clean`;

-- combine all ride data into one table
CREATE TABLE IF NOT EXISTS `cyclistic.trips` AS 
SELECT *
FROM (
  SELECT * FROM `cyclistic.202208_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202209_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202210_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202211_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202212_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202301_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202302_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202303_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202304_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202305_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202306_tripdata`
  UNION ALL
  SELECT * FROM `cyclistic.202307_tripdata`
);

/**************
**  EXPLORE  **
**************/

-- check total rows (5,723,606)
SELECT COUNT(*) AS row_count
FROM `cyclistic.trips`;

-- check length of ride_id (all have length of 16)
SELECT 
  LENGTH(ride_id) AS ride_id_length,
  COUNT(ride_id) AS row_count
FROM `cyclistic.trips`
GROUP BY 1;

-- check if ride_id has any duplicates (returns 0 values)
SELECT COUNT(ride_id) - COUNT(DISTINCT ride_id) AS duplicate_rows
FROM `cyclistic.trips`;

-- check number of nulls per column
SELECT 
  COUNT(*) - COUNT(ride_id) AS ride_id_null,
  COUNT(*) - COUNT(rideable_type) AS rideable_type_null,
  COUNT(*) - COUNT(started_at) AS started_at_null,
  COUNT(*) - COUNT(ended_at) AS ended_at_null,
  COUNT(*) - COUNT(start_station_name) AS start_station_name_null, -- 868,772 null values (15.2% of all rows)
  COUNT(*) - COUNT(start_station_id) AS start_station_id_null, -- 868,904 null values (15.2% of all rows)
  COUNT(*) - COUNT(end_station_name) AS end_station_name_null, -- 925,008 null values (16.2% of all rows)
  COUNT(*) - COUNT(end_station_id) AS end_station_id_null, -- 925,149 null values (16.2% of all rows)
  COUNT(*) - COUNT(start_lat) AS start_lat_null,
  COUNT(*) - COUNT(start_lng) AS start_lng_null,
  COUNT(*) - COUNT(end_lat) AS end_lat_null, -- 6102 null values (0.1% of all rows) 
  COUNT(*) - COUNT(end_lng) AS end_lng_null, -- 6102 null values (0.1% of all rows)
  COUNT(*) - COUNT(member_casual) AS rider_type_null
FROM `cyclistic.trips`;

-- check rows where geo coordinates are zero (returns 10 rows where end coordinates are zero)
SELECT *
FROM `cyclistic.trips`
WHERE 
  start_lat = 0 OR
  start_lng = 0 OR
  end_lat = 0 OR
  end_lng = 0;

-- check how many stations there are (1801)
SELECT 
  COUNT(DISTINCT start_station_name) AS start_station_count, -- 1801 start stations
  COUNT(DISTINCT end_station_name) AS end_station_count, -- 1799 end stations
  COUNT(DISTINCT start_station_id) AS start_station_id_count, -- 1502 start station ids
  COUNT(DISTINCT end_station_id) AS end_station_id_count, -- 1505 end station ids
FROM `cyclistic.trips`; 

-- confirm that member_type is only 2 values: "member" and "casual."
SELECT 
  DISTINCT member_casual AS member_type,
  COUNT(member_casual) AS member_type_count
FROM `cyclistic.trips`
GROUP BY 1;

-- check rideable_type. 3 unique types of bikes
SELECT 
  DISTINCT rideable_type AS bike_type, 
  COUNT(rideable_type) AS trips_count
FROM `cyclistic.trips`
GROUP BY 1;

-- explore geo coordinates and station name matches. discover that station names can have multiple coordinate matches due to varying coordinate decimal lengths
SELECT
  DISTINCT CONCAT(start_lat,', ',start_lng) AS start_coords,
  start_station_name
FROM `cyclistic.trips`
WHERE 
  start_lat IS NOT NULL AND
  start_lng IS NOT NULL AND
  start_station_name IS NOT NULL
ORDER BY 2;

-- isolate start station ids with more than one matching station name. 282 station ids. 
-- of those, 27 station ids have 3 matching station names. the remaining 255 have 2. station ids are mostly 3-digit numbers. 
SELECT
  start_station_id,
  COUNT(*) AS station_name_count
FROM 
  (SELECT
    DISTINCT start_station_name AS station_name,
    start_station_id  
  FROM `cyclistic.trips`
  WHERE 
    start_station_id IS NOT NULL AND
    start_station_name IS NOT NULL
  GROUP BY 1, 2
  ORDER BY 1)
GROUP BY start_station_id
HAVING COUNT(*) > 1;

-- repeat for end station ids
-- isolate end station ids with more than one matching station name. 277 station ids. 
-- of those, 28 station ids have 3 matching station names. the remaining 249 have 2. station ids are mostly 3-digit numbers. 
SELECT
  end_station_id,
  COUNT(*) AS station_name_count
FROM 
  (SELECT
    DISTINCT end_station_name AS station_name,
    end_station_id  
  FROM `cyclistic.trips`
  WHERE 
    end_station_id IS NOT NULL AND
    end_station_name IS NOT NULL
  GROUP BY 1, 2
  ORDER BY 1)
GROUP BY end_station_id
HAVING COUNT(*) > 1;

-- explore cases where station id has 3 matching station names. discover that many duplicates include "public racks," but public rack location not always the same.
-- research to find that public racks are different from bike docks but often appear at the same intersections
SELECT 
  DISTINCT start_station_name,
  start_station_id,
FROM `cyclistic.trips`
WHERE start_station_id IN ('523', '661'); 

-- explore cases where station id matches station name. only 3 cases
SELECT 
  DISTINCT start_station_name,
  start_station_id,
FROM `cyclistic.trips`
WHERE start_station_id = start_station_name;


/************************
**  PROCESS AND CLEAN  **
*************************/

-- drop table if existing
DROP TABLE IF EXISTS `cyclistic.trips_geo_start`;
DROP TABLE IF EXISTS `cyclistic.trips_geo_end`;

-- create geo table for start coordinates, containing geographic attributes for all ride_ids
CREATE TEMP FUNCTION tag_value(tags ARRAY<STRUCT<key STRING, value STRING>>, name STRING)
RETURNS STRING AS (
  (SELECT value FROM UNNEST(tags) WHERE key = name)
);

CREATE TABLE IF NOT EXISTS cyclistic.trips_geo_start AS (
  WITH reference_data AS (
    SELECT 
      CAST(tag_value(all_tags, 'admin_level') AS INT64) as admin_level,
      tag_value(all_tags, 'name') as name,
      geometry
    FROM (
      SELECT all_tags, geometry
      FROM `bigquery-public-data.geo_openstreetmap.planet_features_multipolygons`
      WHERE SAFE_CAST(tag_value(all_tags, 'admin_level') AS INT64) IS NOT NULL
    )
  ), start_points AS (
    SELECT ride_id, ST_GeogPOINT(start_lng, start_lat) as p
    FROM `cyclistic.trips`
  )
  SELECT ride_id, admin_level, name 
  FROM reference_data JOIN start_points
  ON ST_INTERSECTS(geometry, p)
  ORDER BY ride_id, admin_level
);

-- create geo table for end coordinates, containing geographic attributes for all ride_ids
CREATE TABLE IF NOT EXISTS cyclistic.trips_geo_end AS (
  WITH reference_data AS (
    SELECT 
      CAST(tag_value(all_tags, 'admin_level') AS INT64) as admin_level,
      tag_value(all_tags, 'name') as name,
      geometry
    FROM (
      SELECT all_tags, geometry
      FROM `bigquery-public-data.geo_openstreetmap.planet_features_multipolygons`
      WHERE SAFE_CAST(tag_value(all_tags, 'admin_level') AS INT64) IS NOT NULL
    )
  ), end_points AS (
    SELECT ride_id, ST_GeogPOINT(end_lng, end_lat) as p
    FROM `cyclistic.trips`
  )
  SELECT ride_id, admin_level, name 
  FROM reference_data JOIN end_points
  ON ST_INTERSECTS(geometry, p)
  ORDER BY ride_id, admin_level
);

-- drop table if existing
DROP TABLE IF EXISTS `cyclistic.trips_valid`;

-- create new table with added columns, limited to rows with valid data
CREATE TABLE IF NOT EXISTS cyclistic.trips_valid AS (
SELECT
  ride_id, 
  rideable_type AS bike_type, 
  member_casual AS rider_type, 
  REGEXP_REPLACE(TRIM(start_station_name, '*'), 'Public Rack - ', '') AS start_station_name, -- remove asterisks and "public rack" from station names for standardization
  REGEXP_REPLACE(TRIM(end_station_name, '*'), 'Public Rack - ', '') AS end_station_name,
  ROUND(start_lat, 4) AS start_lat, -- round to 4 decimal places, which makes geo coordinates accurate to within 11.1 meters
  ROUND(start_lng, 4) AS start_lng, 
  ROUND(end_lat, 4) AS end_lat, 
  ROUND(end_lng, 4) AS end_lng, 
  started_at,
  ended_at,
  CASE
    WHEN 0 <= EXTRACT(HOUR FROM started_at) AND EXTRACT(HOUR FROM started_at) < 6 THEN 'Night' -- time of day
    WHEN 6 <= EXTRACT(HOUR FROM started_at) AND EXTRACT(HOUR FROM started_at) < 12 THEN 'Morning'
    WHEN 12 <= EXTRACT(HOUR FROM started_at) AND EXTRACT(HOUR FROM started_at) < 18 THEN 'Afternoon' 
    WHEN 18 <= EXTRACT(HOUR FROM started_at) AND EXTRACT(HOUR FROM started_at) < 24 THEN 'Evening' 
    ELSE 'Unknown'
  END AS time_of_day, 
  FORMAT_DATE('%a', started_at) AS day_of_week,-- day of week
  FORMAT_DATE('%d', started_at) AS day, -- day
  FORMAT_DATE('%b', started_at) AS month, -- month
  FORMAT_DATE('%Y', started_at) AS year, -- year
  CONCAT('Q',FORMAT_DATE('%Q', started_at)) AS quarter, -- quarter
  FORMAT_TIMESTAMP("%I:%M %p", started_at) AS start_time, -- ride start time
  TIMESTAMP_DIFF (ended_at, started_at, minute) AS ride_duration, -- ride duration in minutes
  CAST( ST_DISTANCE(
    ST_GEOGPOINT(ROUND(start_lng, 4), ROUND(start_lat, 4)), 
    ST_GEOGPOINT(ROUND(end_lng, 4), ROUND(end_lat, 4))
    ) AS INT64) AS station_distance, -- distance from start station to end station in meters
FROM `cyclistic.trips`
WHERE 
  start_station_name IS NOT NULL AND
  start_station_id IS NOT NULL AND
  end_station_name IS NOT NULL AND
  end_station_id IS NOT NULL AND
  end_lat IS NOT NULL AND
  end_lng IS NOT NULL AND
  end_lat != 0 AND
  end_lng != 0 AND
  TIMESTAMP_DIFF (ended_at, started_at, minute) > 1 AND 
  TIMESTAMP_DIFF (ended_at, started_at, hour) < 24
);

-- drop table if existing
DROP TABLE IF EXISTS `cyclistic.trips_cleaned`;

-- create new table with geo data appended to trips_valid table
CREATE TABLE  `cyclistic.trips_cleaned` AS (
  SELECT v.*, g.name AS end_neighborhood FROM (
    SELECT v.*, g.name AS start_neighborhood
    FROM `cyclistic.trips_valid` AS v
    LEFT JOIN `cyclistic.trips_geo_start` AS g
    ON v.ride_id = g.ride_id
    AND g.admin_level = 10
    ) AS v
  LEFT JOIN `cyclistic.trips_geo_end` AS g
  ON v.ride_id = g.ride_id
  AND g.admin_level = 10
);

-- count rows in 'trips_valid' table (4,175,528). excludes 27% of rows from 'trips' table
SELECT COUNT(*) AS rows_count
FROM `cyclistic.trips_valid`;

-- count rows in 'trips_cleaned' table (returns 4,204,923 rows, or 29,395 more than trips_valid)
SELECT COUNT(*) AS rows_count
FROM `cyclistic.trips_cleaned`;

-- check if ride_id has any duplicates (returns 29,395 values)
SELECT COUNT(ride_id) - COUNT(DISTINCT ride_id) AS duplicate_rows
FROM `cyclistic.trips_cleaned`;

-- explore duplicates (reveals that Armour Square and Chinatown neighborhoods overlap. research confirms that Armour Square neighborhood encompasses Chinatown neighborhood)

SELECT *
FROM `cyclistic.trips_cleaned` a
JOIN (SELECT ride_id, COUNT(*) AS count
FROM `cyclistic.trips_cleaned`
GROUP BY 1
HAVING COUNT(*) > 1) b
ON a.ride_id = b.ride_id
ORDER BY a.ride_id;

-- delete 29,395 duplicate Chinatown rows from 'trips_cleaned' table
DELETE 
FROM `cyclistic.trips_cleaned`
WHERE ride_id IN (
  SELECT a.ride_id
  FROM `cyclistic.trips_cleaned` a
  JOIN (
    SELECT ride_id, COUNT(*) AS count
    FROM `cyclistic.trips_cleaned`
    GROUP BY 1
    HAVING COUNT(*) > 1) b
  ON a.ride_id = b.ride_id
  AND (start_neighborhood = 'Chinatown'
    OR end_neighborhood = 'Chinatown')
  ORDER BY a.ride_id
) AND (start_neighborhood = 'Chinatown'
    OR end_neighborhood = 'Chinatown');

-- count rows in 'trips_cleaned' table (now returns correct number, 4,175,528 rows)
SELECT COUNT(*) AS rows_count
FROM `cyclistic.trips_cleaned`;
