
with 
q1 as (
SELECT *  FROM {{source("tourism", "unwto_outbound_tourism_expenditure")}}
WHERE COALESCE(
    country, type_of_expenditure, units, 
    year_1995, year_1996, year_1997, year_1998, year_1999, year_2000, 
    year_2001, year_2002, year_2003, year_2004, year_2005, 
    year_2006, year_2007, year_2008, year_2009, year_2010, 
    year_2011, year_2012, year_2013, year_2014, year_2015, 
    year_2016, year_2017, year_2018, year_2019, year_2020, 
    year_2021, year_2022
) IS NOT NULL),

q2 as (

select * from q1 
where coalesce(country, type_of_expenditure) is not null 
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
        type_of_expenditure,
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
where type_of_expenditure is not null
),

q6 as (

select 
    country_filled as country,
    type_of_expenditure,
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
    type_of_expenditure,    
    REPLACE (trim(year_column),'YEAR_', '') as year_of_travel,
    Value as total_expenditure_in_US_dollars
from q6
),

final_unwto_outbound_departures
as (

select 
    CAST(country as VARCHAR) as country,
    CAST(type_of_expenditure as VARCHAR) as type_of_expenditure,
    CAST(year_of_travel as INT) as year_of_travel,
    CAST(((TRY_CAST(total_expenditure_in_US_dollars as FLOAT)) * 1000000) AS INT)  AS total_expenditure_in_US_dollars
from q7
)

SELECT * from final_unwto_outbound_departures
where country = 'POLAND'
Order by total_expenditure_in_US_dollars desc


--TRY_CAST(REGEXP_REPLACE(number_of_tourists, '[^0-9]+$', '') as float)

--REGEXP_CONTAINS(number_of_tourists, '[0-9]+[\.[0-9]+])?')
