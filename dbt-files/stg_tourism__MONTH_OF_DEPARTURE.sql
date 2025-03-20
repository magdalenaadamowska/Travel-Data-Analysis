with 
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2013, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2014, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2015, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2016, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2017, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2018, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2019, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2020, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2021, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2022, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2023, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'MONTH_OF_DEPARTURE')}}
GROUP BY frequency, destination, reason, lenght_of_travel, month_of_travel, number_1, country_of_tourist
),


ready_month_of_departure
as (
SELECT destination, lenght_of_travel, month_of_travel, year_of_travel, number_of_travelers
from normalized_month_of_departure
where reason = 'PER' and country_of_tourist = 'PL' and destination in ('DOM', 'FOR') and month_of_travel like 'M%'
)

select * from ready_month_of_departure



