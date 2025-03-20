with 
normalized_age_group
AS (
SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2012' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2013' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2014' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2015' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2016' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2017' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2018' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2019' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2020' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2021' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2022' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist

UNION

SELECT 
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as reason,
    SPLIT_PART(OPIS,',', 3) as destination,
    SPLIT_PART(OPIS,',', 4) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 5) as age_group,
    SPLIT_PART(OPIS,',', 6) as number_1,
    SPLIT_PART(OPIS,',', 7) as country_of_tourist,
    '2023' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM {{source("tourism", 'age_group')}}
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist
),


ready_age_group
as (
SELECT destination, lenght_of_travel, age_group, year_of_travel, number_of_travelers
from normalized_age_group
where reason = 'PER' and country_of_tourist = 'PL' and destination in ('DOM', 'FOR')
)

select * from ready_age_group



