SET tenant_id_dev = 'db2dc657-9dd8-4312-8de1-a2f2100e30d3';

--Create new storage integration by connecting to correct blob storage account
CREATE or REPLACE STORAGE INTEGRATION ah64595_RAW_STORAGE_INTEGRATION2
 TYPE = EXTERNAL_STAGE -- Sets the type of integration as External
 STORAGE_PROVIDER = AZURE -- Designate Azure as the cloud storage provider
 ENABLED = TRUE -- Enables the integration
 AZURE_TENANT_ID = $tenant_id_dev -- Sets the azure_tentant_id equal to pre-set variable
 STORAGE_ALLOWED_LOCATIONS = ('azure://drewblobcapstone.blob.core.windows.net/raw/');

--Check properties to ensure info is correct
DESC STORAGE INTEGRATION ah64595_RAW_STORAGE_INTEGRATION2;

 
 --GRANT full rights to SYSDADMIN --
GRANT OWNERSHIP ON INTEGRATION ah64595_RAW_STORAGE_INTEGRATION2 TO SYSADMIN; 
 
 --Making sure to use the right settings
USE ROLE ACCOUNTADMIN;
USE DATABASE ah64595_dw;
USE WAREHOUSE ah64595_wh;
USE SCHEMA ELT_STAGE;

--Edit the external_stage in your ELT schema.
CREATE OR REPLACE STAGE ELT_STAGE.ELT_RAW_EXTERNAL_STAGE
COMMENT = 'Raw External Stage for the ELT Account on the RRC DataLake Blob Container'
STORAGE_INTEGRATION = ah64595_RAW_STORAGE_INTEGRATION2
URL = 'azure://drewblobcapstone.blob.core.windows.net/raw/';

--Ensuring correct grants on the external stage
SHOW GRANTS ON STAGE ELT_STAGE.ELT_RAW_EXTERNAL_STAGE;
GRANT USAGE ON STAGE ELT_STAGE.ELT_RAW_EXTERNAL_STAGE TO ROLE ACCOUNTADMIN;
GRANT READ, WRITE ON STAGE ELT_STAGE.ELT_RAW_EXTERNAL_STAGE TO ROLE ACCOUNTADMIN;

--View contents of the external stage
LIST @ELT_STAGE.ELT_RAW_EXTERNAL_STAGE/;

--View stats for specific files
LIST @ELT_STAGE.ELT_RAW_EXTERNAL_STAGE/noc_regions.csv; -- View specific file stats
LIST @ELT_STAGE.ELT_RAW_EXTERNAL_STAGE/raw_athlete_event.csv; -- View specific file stats


--Create a table to store noc regions data file
create OR replace transient table ELT_STAGE.noc_regions_dw (
noc string primary key, 
country varchar(50),
notes varchar(50)
);

--Create a table to store the athlete events file
create or replace transient table ELT_STAGE.athlete_events_dw(
id int primary key, 
name string, 
sex string,
age int,
height float,
weight float,
noc string,
games varchar(50),
year varchar(30),
season string,
city string,
sport string,
event string,
medal string
);

-- Ensure file format is good to go
CREATE OR REPLACE FILE FORMAT ELT_STAGE.ELT_CSV_COMMA_DELIMITED_HEADER
COMMENT = 'File Format for CSV comma delimited Column Header files'
COMPRESSION = 'NONE'
TYPE = CSV -- Set file tyle
FIELD_DELIMITER = ',' -- Delimits columns by comma
RECORD_DELIMITER = '\n' -- Delimits rows by line break
SKIP_HEADER = 1 -- Skip the first row and don’t treat as data
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ESCAPE = '\134'
ESCAPE_UNENCLOSED_FIELD = 'NONE'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
EMPTY_FIELD_AS_NULL = TRUE;



TRUNCATE TABLE ELT_STAGE.noc_regions_dw;
---copy from noc regions raw file into table
COPY INTO ELT_STAGE.noc_regions_dw
FROM @ELT_STAGE.ELT_RAW_EXTERNAL_STAGE/noc_regions.csv
FILE_FORMAT = ELT_STAGE.ELT_CSV_COMMA_DELIMITED_HEADER
ON_ERROR=CONTINUE;

--Test that the copy executed correctly 
SELECT * FROM elt_stage.noc_regions_dw;


TRUNCATE TABLE ELT_STAGE.athlete_events_dw;
---copy from athlete events raw file into table
COPY INTO ELT_STAGE.athlete_events_dw
FROM @ELT_STAGE.ELT_RAW_EXTERNAL_STAGE/raw_athlete_event.csv
FILE_FORMAT = (FORMAT_NAME = 'ELT_STAGE.ELT_CSV_COMMA_DELIMITED_HEADER' NULL_IF = 'NA') --some numeric values are strings in the dataset
ON_ERROR=CONTINUE;
--VALIDATION_MODE=RETURN_ALL_ERRORS; used to figure out errors

--Check to see null conversion happened successfully 
SELECT * From elt_stage.athlete_events_dw where age is null and height is null and weight is null;
--about 8.4k records have nulls for age, weight, and height. 


--Create new silver schema for this project
CREATE or REPLACE SCHEMA EDW_SILVER_LAYER2
COMMENT = 'This schema is used to create Silver Layer for capstone project';

--Dedupe noc regions table
CREATE OR REPLACE TABLE elt_stage.deduped_noc_regions AS
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY noc ORDER BY noc) AS row_num
    FROM elt_stage.noc_regions_dw
) AS subquery
WHERE row_num = 1;

-- Dedupe athlete_events table
CREATE OR REPLACE TABLE elt_stage.deduped_athlete_events AS
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id, name, sex, age, height, weight, noc, games, year, season,
        city, sport, event, medal ORDER BY id) AS row_num
    FROM elt_stage.athlete_events_dw
) AS subquery
WHERE row_num = 1;

--Ensure deduplication worked
select * from elt_stage.deduped_noc_regions order by noc; 
select * from elt_stage.deduped_athlete_events order by id;

--Merge the two deduped elt tables in the silver layer
CREATE OR REPLACE TABLE edw_silver_layer2.noc_athlete_events_merge as (
Select 
    nr.noc,
    nr.country, --primary reason for merging, only noc code was in the athlete event table
    ae.id,
    ae.name,
    ae.sex,
    ae.age,
    ae.height,
    ae.weight,
    ae.games,
    ae.year,
    TO_DATE(CONCAT(ae.year, '-01-01'), 'YYYY-MM-DD') AS year_as_date, --alter to datetime for time series analysis 
    ae.season,
    ae.city,
    ae.city || ' ' || ae.year as city_year, --parse column by concatenating
    ae.sport,
    ae.event as sport_event,
    ae.medal,
    CASE
        WHEN ae.medal is not null then 1
        ELSE 0
    END as medal_won --Binary identifier for whether an athlete won a medal for the event
FROM elt_stage.deduped_noc_regions nr inner join elt_stage.deduped_athlete_events ae 
    on nr.noc = ae.noc
);


--Test to make sure changes happened correctly, should produce no results
select * from edw_silver_layer2.noc_athlete_events_merge where medal is null and medal_won = 1;


--Create new gold schema for this project
CREATE or REPLACE SCHEMA EDW_GOLD_LAYER2
COMMENT = 'This schema is used to create Gold Layer for capstone project';

--Create a gold layer table that aggregates from silver table 
CREATE OR REPLACE TABLE edw_gold_layer2.countries_aggregated as (
SELECT 
country,
noc, 
SUM(medal_won) as total_medals,
SUM(CASE when medal = 'Gold' then 1 else 0 end) as total_gold,
SUM(CASE when medal = 'Silver' then 1 else 0 end) as total_silver,
SUM(CASE when medal = 'Bronze' then 1 else 0 end) as total_bronze
FROM edw_silver_layer2.noc_athlete_events_merge
GROUP BY country, noc
ORDER BY total_medals desc
);

--select from the gold layer to find which countries have won the most medals
select * from edw_gold_layer2.countries_aggregated;

