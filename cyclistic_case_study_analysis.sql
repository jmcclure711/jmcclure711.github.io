-- To start, I need to union the 12 monthly files containing the 2023 trip data into one table

CREATE TABLE Capstone_Cyclistic.2023_trip_data AS 
SELECT *
FROM (
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Jan_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Feb_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Mar_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Apr_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.May_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Jun_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Jul_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Aug_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Sep_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Oct_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Nov_2023_trip_data`
      UNION ALL
      SELECT * FROM `singular-backup-413521.Capstone_Cyclistic.Dec_2023_trip_data`
    );

SELECT *
FROM `singular-backup-413521. Capstone_Cyclistic.2023_trip_data;

-- This returned 5,719,877 rows of data which matches the sum of the twelve tables combined, so the union was successful.

---------------------------------------Preliminary Exploration------------------------------------------------------------

/* 1. ride_id
- Check the length opf all ID's and see if there are any outliers.
*/

SELECT
  LENGTH(ride_id) AS id_length,
  COUNT(*) AS #_of_ids
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
GROUP BY LENGTH(ride_id)

-- Check to see if all ID's are unique.

SELECT
  COUNT(DISTINCT ride_id) AS distinct_ids
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`

-- There are 5,719,877 ID's, meaning all are distinct.
--------------------------------------------------------------------------------------------
/* 2. rideable_type
- Check the number of different types of bikes offered and the amount of trips taken with each.
*/

SELECT
  rideable_type,
  COUNT(rideable_type) AS biketype_trips
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
GROUP BY rideable_type

-- There are three different types of bikes: electric, classic and docked. Most popular is electric, followed by classic and then docked as a distant third.

--------------------------------------------------------------------------------------------
/* 3. start_station/end_station
- Check for naming inconsistencies or null values
*/

SELECT
  start_station_name,
  COUNT(*) AS starting_trips
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
GROUP BY start_station_name
ORDER BY start_station_name;

SELECT
  end_station_name,
  COUNT(*) AS ending_trips
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
GROUP BY end_station_name
ORDER BY end_station_name;

-- There are over 875,000 null values for start_station_names and over 900,000 null values for end_station_names.
-- Over 400,000 of these instances occur where both are null at once. This information is too incomplete to use in my analysis in my opinion, so I will drop it from the set.

----------------------------------------------------------------------------------------------

/* 4. member_casual
- Will confirm there are only two rider types and check the amount of each through the year.
*/

SELECT 
  DISTINCT(member_casual)
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`;

SELECT 
  COUNT(member_casual) AS member_rides
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
WHERE member_casual = 'member';

SELECT
  COUNT(member_casual) AS casual_rides
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
WHERE member_casual = 'casual';

/* It is confirmed there are only member and casual rider types.
- There's record of 3,660,698 rides from members and 2,059,179 rides from casual customers
*/

-----------------------------------------------------------------------------------------------

/* 5. start_time + end_time
- Check for outliers for trip duration (under a minute and over a day)
*/

SELECT
  COUNT(*) AS outlier_trips
FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data`
WHERE 
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 1
  OR TIMESTAMP_DIFF(ended_at, started_at, MINUTE) >= 1440

-- There are 269,711 trips that are outliers and will be removed from the data. 

-------------------------------------BEGIN DATA CLEANING AND ANALYSIS----------------------------------------------------------
/*
1. First, I will be creating the cleaned dataset from a temporary table containing extracted date values, then filtering out the outlier trips and null station names.
*/

WITH extracted_tripdata AS (
   SELECT 
      ride_id,
      started_at,
      ended_at,
      rideable_type,
      start_station_name,
      end_station_name,
      CASE
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'Sunday'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'Monday'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'Tuesday'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'Wednesday'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'Thursday'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'Friday'
         ELSE'Saturday' 
      END AS day_of_week,
      CASE
         WHEN EXTRACT(MONTH FROM started_at) = 1 THEN 'January'
         WHEN EXTRACT(MONTH FROM started_at) = 2 THEN 'February'
         WHEN EXTRACT(MONTH FROM started_at) = 3 THEN 'March'
         WHEN EXTRACT(MONTH FROM started_at) = 4 THEN 'April'
         WHEN EXTRACT(MONTH FROM started_at) = 5 THEN 'May'
         WHEN EXTRACT(MONTH FROM started_at) = 6 THEN 'June'
         WHEN EXTRACT(MONTH FROM started_at) = 7 THEN 'July'
         WHEN EXTRACT(MONTH FROM started_at) = 8 THEN 'August'
         WHEN EXTRACT(MONTH FROM started_at) = 9 THEN 'September'
         WHEN EXTRACT(MONTH FROM started_at) = 10 THEN 'October'
         WHEN EXTRACT(MONTH FROM started_at) = 11 THEN 'November'
         ELSE 'December'
      END AS month,
      EXTRACT(DAY FROM started_at) as day,
      EXTRACT(YEAR FROM started_at) AS year,
      TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_time_minutes,
      member_casual AS member_type
   FROM `singular-backup-413521.Capstone_Cyclistic.2023_trip_data` 
),
cleaned_tripdata AS (
SELECT
  *
FROM
  extracted_tripdata
WHERE
  ride_time_minutes BETWEEN 1 AND 1440 
  AND start_station_name IS NOT NULL
  AND end_station_name IS NOT NULL 
),

-- This leaves a total of 4,258,846 trips to conduct analysis on.

-------------------------------------------ANALYSIS-------------------------------------------------------------

/*
1. Bike Type: Find out the different bikes used per customer type
*/

bike_type_data AS(
  SELECT
    rideable_type AS bike_type,
    member_casual AS member_type,
    COUNT(*) AS num_of_trips
  FROM cleaned_tripdata
  GROUP BY 
    rideable_type,
    member_casual
),

-----------------------------------------------------------------------------------------------

/*
2. Rides per month: How many rides were taken per month by both customer types?
*/

rides_per_month AS (
  SELECT
    month,
    member_casual AS member_type,
    COUNT(*) AS num_of_trips
  FROM cleaned_tripdata
  GROUP BY
    month,
    member_type
  ORDER BY month
),

-------------------------------------------------------------------------------------------------

/*
3. Rides per day: What was the ride distribution per each customer type by day of the week?
*/

rides_per_day AS (
  SELECT
   day_of_week,
   member_casual AS member_type,
   COUNT(*) AS num_of_trips
  FROM cleaned_tripdata
  GROUP BY 
    day_of_week,
    member_type
  ORDER BY 
    day_of_week 
),

--------------------------------------------------------------------------------------------------

/*
4. Rides per hour: What was the distribution of rides per hour of the day among both customer types?
*/

rides_per_hour AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS time_of_day,
    member_casual AS member_type,
    COUNT(*) AS num_of_trips
  FROM cleaned_tripdata
  GROUP BY
    member_type,
    time_of_day
), 

---------------------------------------------------------------------------------------------------

/*
5. Average trip durations: What is the difference among how long each ride took between each customer type?
- First, avg trip duration per day of the week.
*/

avg_trip_duration_by_day AS (
  SELECT
    member_casual AS member_type,
    day_of_week,
    ROUND(AVG(ride_time_minutes),1) AS avg_trip_duration,
    COUNT(*) AS num_of_trips
  FROM cleaned_tripdata
  GROUP BY
    member_type,
    day_of_week
),

-- Next, avg trip duration by month.

avg_trip_duration_by_month AS (
  SELECT
    member_casual AS member_type,
    month,
    ROUND(AVG(ride_time_minutes),1) AS avg_month_trip_duration
  FROM
    cleaned_tripdata
  GROUP BY 
    member_type,
    month
)

-- Finally, overall avg trip duration.

SELECT
  member_casual AS member_type,
  ROUND(AVG(ride_time_minutes),1) AS avg_trip_duration
FROM cleaned_tripdata
GROUP BY 
  member_type


