with 
q1 as (
SELECT *  FROM {{source("tourism", "UNWTO_INBOUND_REGIONS")}}
WHERE COALESCE(
    country, region_of_tourists, units, 
    year_1995, year_1996, year_1997, year_1998, year_1999, year_2000, 
    year_2001, year_2002, year_2003, year_2004, year_2005, 
    year_2006, year_2007, year_2008, year_2009, year_2010, 
    year_2011, year_2012, year_2013, year_2014, year_2015, 
    year_2016, year_2017, year_2018, year_2019, year_2020, 
    year_2021, year_2022
) IS NOT NULL),

q2 as (

select * from q1 
where coalesce(country, region_of_tourists) is not null 
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
        region_of_tourists,
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
where region_of_tourists is not null
),

q6 as (

select 
    country_filled as country,
    region_of_tourists,
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
    region_of_tourists,    
    REPLACE (trim(year_column),'YEAR_', '') as year_of_travel,
    Value as number_of_tourists
from q6
),

final_unwto_inbound_regions
as (

select 
    CAST(country as VARCHAR) as country,
    CAST(region_of_tourists as VARCHAR) as region_of_tourists,
    CAST(year_of_travel as INT) as year_of_travel,
    CAST(((TRY_CAST(number_of_tourists as FLOAT)) * 1000) AS INT)  AS number_of_tourists
from q7
)

SELECT * from final_unwto_inbound_regions
where country = 'POLAND'
Order by number_of_tourists desc


--TRY_CAST(REGEXP_REPLACE(number_of_tourists, '[^0-9]+$', '') as float)

--REGEXP_CONTAINS(number_of_tourists, '[0-9]+[\.[0-9]+])?')
