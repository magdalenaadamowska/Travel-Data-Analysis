with

source_group
AS (
SELECT * FROM {{source("tourism", 'age_group')}}
),

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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2012, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2013, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2014, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2015, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2016, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2017, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2018, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2019, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2020, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2021, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2022, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
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
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2023, '[^0-9]+$', '') as integer)) AS number_of_nights_spent
FROM source_group
GROUP BY frequency, reason, destination, lenght_of_travel, age_group, number_1, country_of_tourist
),


ready_age_group
as (
SELECT destination, lenght_of_travel, age_group, year_of_travel, number_of_nights_spent
from normalized_age_group
where reason = 'PER' and country_of_tourist = 'PL' and destination in ('DOM', 'FOR')
),

ready_age_group_names
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
    CASE
        WHEN age_group = 'Y_LT15' THEN '<15 years'
        WHEN age_group = 'Y15-24' THEN '15-24 years'
        WHEN age_group = 'Y25-34' THEN '25-34 years'
        WHEN age_group = 'Y35-44' THEN '35-44 years'
        WHEN age_group = 'Y45-54' THEN '45-54 years'
        WHEN age_group = 'Y55-64' THEN '55-64 years'
        WHEN age_group = 'Y_GE65' THEN '>65 years'
        else 'else groups'
    END as age_group,

    year_of_travel as year_of_trip,
    number_of_nights_spent
from ready_age_group
)

select * from ready_age_group_names
where age_group != 'else groups' and lenght_of_trip in ('1-3 nights', '4 nights and over')
