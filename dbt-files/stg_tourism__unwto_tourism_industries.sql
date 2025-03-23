
with
q1 as (
SELECT *  FROM {{source("tourism", "unwto_tourism_industries")}}
WHERE COALESCE(
    country, accommontations, units,
    year_1995, year_1996, year_1997, year_1998, year_1999, year_2000,
    year_2001, year_2002, year_2003, year_2004, year_2005,
    year_2006, year_2007, year_2008, year_2009, year_2010,
    year_2011, year_2012, year_2013, year_2014, year_2015,
    year_2016, year_2017, year_2018, year_2019, year_2020,
    year_2021, year_2022
) IS NOT NULL),

q2 as (

select * from q1
where coalesce(country, accommontations) is not null
),
q3 as (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_number_1, *
        FROM q2
),
q4 as (

SELECT
        LAST_VALUE(country IGNORE NULLS) OVER (
           ORDER BY row_number_1
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS country_filled,
        accommontations,
        units,
        year_1995, year_1996, year_1997, year_1998, year_1999,
        year_2000, year_2001, year_2002, year_2003, year_2004,
        year_2005, year_2006, year_2007, year_2008, year_2009,
        year_2010, year_2011, year_2012, year_2013, year_2014,
        year_2015, year_2016, year_2017, year_2018, year_2019,
        year_2020, year_2021, year_2022,
        row_number_1
from q3),

q5 as(

select * from q4
where accommontations is not null
),

q6 as (

select
    country_filled as country,
    accommontations,
    units,
    year_column,
    value
from q5
UNPIVOT (value FOR year_column IN (year_1995, year_1996, year_1997, year_1998, year_1999,
        year_2000, year_2001, year_2002, year_2003, year_2004,
        year_2005, year_2006, year_2007, year_2008, year_2009,
        year_2010, year_2011, year_2012, year_2013, year_2014,
        year_2015, year_2016, year_2017, year_2018, year_2019,
        year_2020, year_2021, year_2022))

),
q7 as (
select
    country,
    accommontations,
    units,
    REPLACE (trim(year_column),'YEAR_', '') as year_of_travel,
    Value as accommontations_value
from q6
),

final_unwto_tourism_industries
as (

select
    CAST(country as VARCHAR) as country,
    CAST(accommontations as VARCHAR) as accommontations,
    CAST(units as VARCHAR) as units,
    CAST(year_of_travel as INT) as year_of_travel,
    (TRY_CAST(accommontations_value as FLOAT)) AS accommontations_value
from q7
)

SELECT * from final_unwto_tourism_industries
where country = 'POLAND'
ORDER BY year_of_travel



--TRY_CAST(REGEXP_REPLACE(number_of_tourists, '[^0-9]+$', '') as float)

--REGEXP_CONTAINS(number_of_tourists, '[0-9]+[\.[0-9]+])?')
