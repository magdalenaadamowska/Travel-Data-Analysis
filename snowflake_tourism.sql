CREATE DATABASE TOURISM;
CREATE SCHEMA TOURISM.RAW;
CREATE SCHEMA TOURISM.EXTERNAL_STAGES;
CREATE SCHEMA TOURISM.FILE_FORMATS;

CREATE FILE FORMAT TOURISM.FILE_FORMATS.tsv_format
    TYPE = CSV
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null','')
    EMPTY_FIELD_AS_NULL = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD';
    
CREATE OR REPLACE FILE FORMAT TOURISM.FILE_FORMATS.csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 3
    NULL_IF = ('NULL', 'null','')
    EMPTY_FIELD_AS_NULL = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD';
    
CREATE STAGE TOURISM.external_stages.s3_stage
URL = 's3://s3-tourism-data'
CREDENTIALS = (AWS_KEY_ID = 'KEYAAAAAAAA' AWS_SECRET_KEY = 'SECRETAAAA');

LIST @TOURISM.EXTERNAL_STAGES.s3_stage;

CREATE OR REPLACE TABLE TOURISM.RAW.month_of_departure(
    opis string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string,
    year_2023 string
);

COPY INTO TOURISM.RAW.month_of_departure
    FROM @TOURISM.EXTERNAL_STAGES.s3_stage/month_of_departure.tsv
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.tsv_format)
    ON_ERROR='CONTINUE';

SELECT * from TOURISM.RAW.month_of_departure;


CREATE OR REPLACE TABLE TOURISM.RAW.age_group(
    opis string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string,
    year_2023 string
);

COPY INTO TOURISM.RAW.age_group
    FROM @TOURISM.EXTERNAL_STAGES.s3_stage/age_group.tsv
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.tsv_format)
    ON_ERROR='CONTINUE';

SELECT * from TOURISM.RAW.age_group;

SELECT OPIS, YEAR_2012 FROM TOURISM.RAW.month_of_departure
WHERE OPIS LIKE '%M01%' AND OPIS LIKE '%PL%';



CREATE OR REPLACE TABLE TOURISM.RAW.unwto_inbound_regions(
    country string,
    region_of_tourists string,
    units string,
    year_1995 string,
    year_1996 string,
    year_1997 string,
    year_1998 string,
    year_1999 string,
    year_2000 string,
    year_2001 string,
    year_2002 string,
    year_2003 string,
    year_2004 string,
    year_2005 string,
    year_2006 string,
    year_2007 string,
    year_2008 string,
    year_2009 string,
    year_2010 string,
    year_2011 string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string
);

COPY INTO TOURISM.RAW.unwto_inbound_regions
    FROM (SELECT $4, $7, $9, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39 FROM @TOURISM.EXTERNAL_STAGES.s3_stage/unwto_inbound_tourism_regions.csv)
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.csv_format)
    ON_ERROR='CONTINUE';
TRUNCATE TABLE TOURISM.RAW.unwto_inbound_regions;

Select * from TOURISM.RAW.unwto_inbound_regions;
SHOW TABLES IN TOURISM.RAW;
SHOW GRANTS ON TABLE TOURISM.RAW.UNWTO_INBOUND_REGIONS;
SHOW GRANTS ON TABLE TOURISM.RAW.month_of_departure;

GRANT USAGE ON SCHEMA TOURISM.RAW TO ROLE PC_DBT_DB_PICKER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA TOURISM.RAW TO ROLE PC_DBT_DB_PICKER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TOURISM.RAW TO ROLE PC_DBT_DB_PICKER_ROLE;


CREATE OR REPLACE TABLE TOURISM.RAW.unwto_outbound_tourism_departures(
    country string,
    type_of_travel string,
    units string,
    year_1995 string,
    year_1996 string,
    year_1997 string,
    year_1998 string,
    year_1999 string,
    year_2000 string,
    year_2001 string,
    year_2002 string,
    year_2003 string,
    year_2004 string,
    year_2005 string,
    year_2006 string,
    year_2007 string,
    year_2008 string,
    year_2009 string,
    year_2010 string,
    year_2011 string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string
);

COPY INTO TOURISM.RAW.unwto_outbound_tourism_departures
    FROM (SELECT $4, $7, $9, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38 FROM @TOURISM.EXTERNAL_STAGES.s3_stage/unwto_outbound_tourism_departures.csv)
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.csv_format)
    ON_ERROR='CONTINUE';


select * from TOURISM.RAW.unwto_outbound_tourism_departures;

CREATE OR REPLACE TABLE TOURISM.RAW.unwto_outbound_tourism_expenditure(
    country string,
    type_of_expenditure string,
    units string,
    year_1995 string,
    year_1996 string,
    year_1997 string,
    year_1998 string,
    year_1999 string,
    year_2000 string,
    year_2001 string,
    year_2002 string,
    year_2003 string,
    year_2004 string,
    year_2005 string,
    year_2006 string,
    year_2007 string,
    year_2008 string,
    year_2009 string,
    year_2010 string,
    year_2011 string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string
);

COPY INTO TOURISM.RAW.unwto_outbound_tourism_expenditure
    FROM (SELECT $4, $6, $9, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40 FROM @TOURISM.EXTERNAL_STAGES.s3_stage/unwto_outbound_tourism_expenditure.csv)
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.csv_format)
    ON_ERROR='CONTINUE';


select * from TOURISM.RAW.unwto_outbound_tourism_expenditure;

CREATE OR REPLACE TABLE TOURISM.RAW.unwto_tourism_industries(
    country string,
    accommontations string,
    units string,
    year_1995 string,
    year_1996 string,
    year_1997 string,
    year_1998 string,
    year_1999 string,
    year_2000 string,
    year_2001 string,
    year_2002 string,
    year_2003 string,
    year_2004 string,
    year_2005 string,
    year_2006 string,
    year_2007 string,
    year_2008 string,
    year_2009 string,
    year_2010 string,
    year_2011 string,
    year_2012 string,
    year_2013 string,
    year_2014 string,
    year_2015 string,
    year_2016 string,
    year_2017 string,
    year_2018 string,
    year_2019 string,
    year_2020 string,
    year_2021 string,
    year_2022 string
);

COPY INTO TOURISM.RAW.unwto_tourism_industries
    FROM (SELECT $4, $6, $9, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39 FROM @TOURISM.EXTERNAL_STAGES.s3_stage/unwto_tourism_industries.csv)
    FILE_FORMAT = (FORMAT_NAME=TOURISM.FILE_FORMATS.csv_format)
    ON_ERROR='CONTINUE';


select * from TOURISM.RAW.unwto_tourism_industries;
