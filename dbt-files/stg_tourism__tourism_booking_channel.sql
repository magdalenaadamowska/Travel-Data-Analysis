with

source_group
AS (
SELECT * FROM {{source("tourism", 'tourism_booking_channel')}}
)

,

normalized_age_group
AS (
SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as type_of_arrangement,
    SPLIT_PART(OPIS,',', 3) as if_web,
    SPLIT_PART(OPIS,',', 4) as destination,
    SPLIT_PART(OPIS,',', 5) as reason,
    SPLIT_PART(OPIS,',', 6) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 7) as number_1,
    SPLIT_PART(OPIS,',', 8) as country_of_tourist,
    '2020' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2020, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM source_group
GROUP BY frequency, type_of_arrangement, if_web, destination, reason, lenght_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as type_of_arrangement,
    SPLIT_PART(OPIS,',', 3) as if_web,
    SPLIT_PART(OPIS,',', 4) as destination,
    SPLIT_PART(OPIS,',', 5) as reason,
    SPLIT_PART(OPIS,',', 6) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 7) as number_1,
    SPLIT_PART(OPIS,',', 8) as country_of_tourist,
    '2021' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2021, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM source_group
GROUP BY frequency, type_of_arrangement, if_web, destination, reason, lenght_of_travel, number_1, country_of_tourist
UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as type_of_arrangement,
    SPLIT_PART(OPIS,',', 3) as if_web,
    SPLIT_PART(OPIS,',', 4) as destination,
    SPLIT_PART(OPIS,',', 5) as reason,
    SPLIT_PART(OPIS,',', 6) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 7) as number_1,
    SPLIT_PART(OPIS,',', 8) as country_of_tourist,
    '2022' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2022, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM source_group
GROUP BY frequency, type_of_arrangement, if_web, destination, reason, lenght_of_travel, number_1, country_of_tourist

UNION

SELECT
    SPLIT_PART(OPIS,',', 1) as frequency,
    SPLIT_PART(OPIS,',', 2) as type_of_arrangement,
    SPLIT_PART(OPIS,',', 3) as if_web,
    SPLIT_PART(OPIS,',', 4) as destination,
    SPLIT_PART(OPIS,',', 5) as reason,
    SPLIT_PART(OPIS,',', 6) as lenght_of_travel,
    SPLIT_PART(OPIS,',', 7) as number_1,
    SPLIT_PART(OPIS,',', 8) as country_of_tourist,
    '2023' as year_of_travel,
    SUM(TRY_CAST(REGEXP_REPLACE(YEAR_2023, '[^0-9]+$', '') as integer)) AS number_of_travelers
FROM source_group
GROUP BY frequency, type_of_arrangement, if_web, destination, reason, lenght_of_travel, number_1, country_of_tourist

),


ready_booking_channel
as (
SELECT type_of_arrangement, if_web, destination, lenght_of_travel, year_of_travel, number_of_travelers
from normalized_age_group
where reason = 'PER' and country_of_tourist = 'PL' and destination in ('DOM', 'FOR')
)


select * from ready_booking_channel
