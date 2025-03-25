with

source_group
AS (
SELECT * FROM {{source("tourism", 'month_of_departure')}}
),

normalized_month_of_departure
AS (
SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2012' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2013' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2013, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2014' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2014, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2015' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2015, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2016' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2016, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2017' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2017, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2018' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2018, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2019' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2019, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2020' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2020, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2021' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2021, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2022' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2022, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as destination,
    SPLIT_PART(OPIS,',', 3) as reason,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as month_of_travel,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2023' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2023, '[^0-9]+$', '') as integer)) AS number_of_trips
FROM source_group
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist
),


ready_month_of_departure
as (
SELECT destination, lenght_of_travel, month_of_travel, year_of_travel, number_of_trips
from normalized_month_of_departure
where reason = 'PER' and country_of_tourist = 'PL' and destination in ('DOM', 'FOR') and month_of_travel like 'M%'
),

ready_month_of_departure_names
as (
select
    case
        WHEN destination = 'DOM' then 'Domestic'
        WHEN destination = 'FOR' then 'Foreign'
    END as Destination,
    case
        WHEN lenght_of_travel = 'N_GE1' THEN '1 night and over'
        WHEN lenght_of_travel = 'N1-3' THEN '1-3 nights'
        WHEN lenght_of_travel = 'N_GE4' THEN '4 nights and over'
    END as lenght_of_trip,
    case
        WHEN month_of_travel = 'M01' THEN 'January'
        WHEN month_of_travel = 'M02' THEN 'February'
        WHEN month_of_travel = 'M03' THEN 'March'
        WHEN month_of_travel = 'M04' THEN 'April'
        WHEN month_of_travel = 'M05' THEN 'May'
        WHEN month_of_travel = 'M06' THEN 'June'
        WHEN month_of_travel = 'M07' THEN 'July'
        WHEN month_of_travel = 'M08' THEN 'August'
        WHEN month_of_travel = 'M09' THEN 'September'
        WHEN month_of_travel = 'M10' THEN 'October'
        WHEN month_of_travel = 'M11' THEN 'November'
        WHEN month_of_travel = 'M12' THEN 'December'
    END as month_of_trip,
    year_of_travel as year_of_trip,
    number_of_trips
from ready_month_of_departure
WHERE lenght_of_trip != '1 day and over')

select * from ready_month_of_departure_names

--select * from ready_month_of_departure
--where year_of_travel = '2020' and month_of_travel = 'M01'
--order by destination
