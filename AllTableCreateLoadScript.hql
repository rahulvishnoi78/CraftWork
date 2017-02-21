SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE DATABASE IF NOT EXISTS userclickstreams;
-- drop table userclickstreams.clickstream_staging;
-- drop table userclickstreams.clickstream;

-- Creating browser dim table
-- Ignoring first column as column title and expected is SCD1.
CREATE EXTERNAL TABLE IF NOT EXISTS userclickstreams.dim_browser (browser_id INT, browser_name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");
-- Load browser table with S3 location as overwrite.
LOAD DATA INPATH '${INPUT}metadata/browser/' OVERWRITE INTO TABLE userclickstreams.dim_browser;

-- Creating operating system dim table
-- Ignoring first column as column title and expected is SCD1.
CREATE EXTERNAL TABLE IF NOT EXISTS userclickstreams.dim_operatingsystem (operating_system_id INT, operating_system STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");
-- Load operating system table from S3 location as Overwrite data to existing
LOAD DATA INPATH '${INPUT}metadata/operating_system/' OVERWRITE INTO TABLE userclickstreams.dim_operatingsystem ;

-- Creating Click Stream Fact as staging External table Table
-- Ignoring first column as column title 
-- Using OpenCSV SerDe to parse data since user_agent contains plenty of comma and single quote's
CREATE EXTERNAL TABLE IF NOT EXISTS userclickstreams.clickstream_staging (trans_id STRING, visitor_id STRING, user_agent_id STRING, browser_id STRING, operating_system_id STRING, resolution_id STRING, hit_timestamp STRING, visit_num STRING, page_event STRING, user_agent STRING, referrer STRING, duration_on_page_seconds STRING, load_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"     = '"',
"escapeChar"    = "\\",
"skip.header.line.count" = "1"
);
-- Append Data from S3 location into every schedule run
LOAD DATA INPATH '${INPUT}data/raw/' OVERWRITE INTO TABLE userclickstreams.clickstream_staging; 

-- Creating Click Stream Fact Table, it will be used in All Analytical queries.
-- Its partitioned by hit_timestamp datetime and output is stored as Parquet into S3 location.
CREATE EXTERNAL TABLE IF NOT EXISTS userclickstreams.clickstream (trans_id BIGINT, visitor_id BIGINT, user_agent_id BIGINT, browser_id BIGINT, operating_system_id BIGINT, resolution_id BIGINT, hit_timestamp timestamp, visit_num INT, page_event INT, user_agent STRING, referrer STRING, duration_on_page_seconds INT, load_date DATE, operating_system_name STRING, operating_system_major STRING, browser_name STRING, browser_major STRING, hit_endTime TIMESTAMP,
ua_devicename STRING, ua_devicetype STRING, ua_os_version  STRING, ua_os_major  STRING)
PARTITIONED BY (dt string)
STORED AS PARQUET 
LOCATION '${OUTPUT}data/cooked2/';

--- Following are few key points 
-- 1. Only Append records
-- 2. Parsing Browser name with split to fetch first part as Major browser. Sampled from provided data.
-- 3. Parseing 
-- 4. Partition on hit_timestamp but not on load_date as 3 last records hit_timestamp are different then load_date
INSERT INTO TABLE userclickstreams.clickstream PARTITION (dt)
SELECT
CAST(trans_id AS BIGINT) AS trans_id,
CAST(visitor_id  AS BIGINT) AS visitor_id, 
CAST(user_agent_id  AS BIGINT) AS user_agent_id, 
CAST(cs.browser_id  AS BIGINT) AS browser_id, 
CAST(o.operating_system_id  AS BIGINT) AS operating_system_id, 
CAST(resolution_id  AS BIGINT) AS resolution_id, 
from_unixtime(unix_timestamp(hit_timestamp, 'yyyy-MM-dd HH:mm:ss')) AS hit_timestamp,
CAST(visit_num AS INT) AS visit_num, 
CAST(page_event AS INT) AS page_event, 
user_agent, 
referrer, 
CAST(duration_on_page_seconds AS INT) AS duration_on_page_seconds, 
TO_DATE(from_unixtime(unix_timestamp(load_date ,'yyyyMMdd'))) AS load_date, 
o.operating_system, lower(regexp_extract(o.operating_system, '[A-Za-z]+', 0)), 
b.browser_name, lower(split(b.browser_name, " ")[0]), 
(unix_timestamp(hit_timestamp, 'yyyy-MM-dd HH:mm:ss') + duration_on_page_seconds) AS hit_endTime,
get_json_object(replace(replace(replace(user_agent, "'", '"'), "(", "["), ")", "]"), "$.Device[0]") AS ua_devicename,
get_json_object(replace(replace(replace(user_agent, "'", '"'), "(", "["), ")", "]"), "$.Device[1]") AS ua_devicetype,
get_json_object(replace(replace(replace(user_agent, "'", '"'), "(", "["), ")", "]"), "$.Operating System[0]") AS ua_os_version,
get_json_object(replace(replace(replace(user_agent, "'", '"'), "(", "["), ")", "]"), "$.Operating System[1]") AS ua_os_major,
from_unixtime(unix_timestamp(hit_timestamp, 'yyyy-MM-dd'), 'yyyyMMdd') AS dt
FROM userclickstreams.clickstream_staging cs
LEFT JOIN userclickstreams.dim_browser b ON CAST(cs.browser_id AS INT) = b.browser_id
LEFT JOIN userclickstreams.dim_operatingsystem  o on CAST(cs.operating_system_id AS INT) = o.operating_system_id;
