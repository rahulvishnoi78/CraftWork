3) How many transactions occurred between September 25, 2013 and September 30, 2013 inclusive? Note: Each record in the file is one transaction. The hit_timestamp column contains the transaction date.
select COUNT(trans_id) As Trans_Count from userclickstreams.clickstream where dt >= "20130925" and dt <= "20130930"	--> Result 1487
-----------------------
4) What is the average length in seconds of all the transactions that occurred between September 25, 2013 and September 30, 2013 inclusive?

If multiple transactions per single user overlap, do not double-count the overlapping transaction length. Only count the overlapping transaction length for the transaction that started first (or has lower transaction id if there are multiple transactions which started at the same time).
See Appendix A for an example.

SELECT AVG(nonoverlappeddurationseconds) FROM (
SELECT trans_id, visitor_id,
IF ((previous_unix_hit_endtimestamp - current_unix_hit_timestamp) > 0, duration_on_page_seconds - (previous_unix_hit_endtimestamp - current_unix_hit_timestamp), duration_on_page_seconds) as nonoverlappeddurationseconds
from (select trans_id, visitor_id,
unix_timestamp(hit_timestamp) as current_unix_hit_timestamp, 
unix_timestamp(LAG(hit_endtime) OVER (PARTITION BY visitor_id Order by hit_timestamp)) as previous_unix_hit_endtimestamp,
COALESCE(CAST(duration_on_page_seconds as INT), 0) as duration_on_page_seconds
from userclickstreams.clickstream 
where dt >= "20130925" and dt <= "20130930") iq1) iq2;
-- > 43.994620040349695
--- Debug Query to test results
drop table query41;
create table query41 (
trans_id BIGINT, visitor_id BIGINT, hit_timestamp STRING, previous_hit_timestamp STRING, 
current_unix_hit_timestamp BIGINT, previous_unix_hit_timestamp BIGINT, hit_endtime BIGINT, duration_on_page_seconds BIGINT, 
NonOverlaped BIGINT);

INSERT INTO  query41
SELECT trans_id, visitor_id, hit_timestamp, previous_hit_endtimestamp,
current_unix_hit_timestamp, previous_unix_hit_endtimestamp, hit_endtime, duration_on_page_seconds,
IF ((previous_unix_hit_endtimestamp - current_unix_hit_timestamp) > 0, duration_on_page_seconds - (previous_unix_hit_endtimestamp - current_unix_hit_timestamp), duration_on_page_seconds) as NonOverlaped
from (select trans_id, visitor_id, hit_timestamp, 
LAG(hit_endtime) OVER (PARTITION BY visitor_id Order by hit_timestamp) as previous_hit_endtimestamp, 
unix_timestamp(hit_timestamp) as current_unix_hit_timestamp, 
unix_timestamp(LAG(hit_endtime) OVER (PARTITION BY visitor_id Order by hit_timestamp)) as previous_unix_hit_endtimestamp, 
hit_endtime,
COALESCE(CAST(duration_on_page_seconds as INT), 0) as duration_on_page_seconds
from userclickstreams.clickstream 
where dt >= "20130925" and dt <= "20130930") iq;

select * from query41 order by visitor_id;

-----------------------
5) How many unique visitors used the Chrome browser (any version)? 
Note: One visitor may have multiple transactions in the file.

SELECT COUNT(distinct visitor_id) as unique_chrome_visitors FROM userclickstreams.clickstream where LOWER(browser_major) == "chrome"; 
---> 1972
-----------------------
6) What was the most used operating system & browser combination for all dates included in the clickstream data?

select Count(trans_id) As TotalTransaction, ua_os_major, browser_major from userclickstreams.clickstream  group by ua_os_major, browser_major order by TotalTransaction DESC limit 10;

TotalTransaction	ua_os_major	browser_major
1784	Android	chrome
1235	Android	opera
436	OS X	chrome
295	Android	firefox
278	OS X	opera
232	Android	edge
223	iOS	opera
185	iOS	chrome
95	OS X	firefox
56	iOS	firefox
-----------------------
7) How many transactions were done using the Macintosh operating system? 

	select COUNT(distinct trans_id) from userclickstreams.clickstream where lower(ua_os_major) = "os x"; 
	-- > 870
-----------------------
8) How many transactions were done using a Desktop Device? 
	select COUNT(distinct trans_id) from userclickstreams.clickstream where LOWER(ua_devicetype) = "desktop"; 
	-- > 925
-----------------------
9) How many unique visitors had transactions per day per major browser? For example, for "Chrome 28.7”, the major browser would be "Chrome”.

select count(distinct visitor_id) as Unique_visitor, dt, browser_major from userclickstreams.clickstream group by dt, browser_major ORDER BY dt;

unique_chrome_visitors dt browser_major
2	11110101	unknown
14	20130916	chrome
10	20130916	firefox
22	20130916	opera
38	20130917	chrome
11	20130917	firefox
34	20130917	opera
20	20130918	chrome
3	20130918	edge
9	20130918	firefox
26	20130918	opera
18	20130920	chrome
3	20130920	firefox
13	20130920	opera
11	20130925	edge
90	20130925	chrome
19	20130925	firefox
43	20130925	opera
1	20130925	safari
37	20130925	unknown
100	20130926	chrome
12	20130926	edge
36	20130926	firefox
106	20130926	opera
135	20130928	chrome
4	20130928	edge
28	20130928	firefox
83	20130928	opera
1	20130928	safari
2	20130928	unknown
299	20130930	chrome
40	20130930	edge
89	20130930	firefox
262	20130930	opera
2	20130930	safari
1	20131001	aol
115	20131001	chrome
15	20131001	edge
31	20131001	firefox
86	20131001	opera
1	20131001	safari
2	20131001	unknown
475	20131002	chrome
122	20131002	edge
40	20131002	firefox
324	20131002	opera
1	20131002	safari
3	20131002	unknown
1	20131003	aol
279	20131003	chrome
65	20131003	edge
21	20131003	firefox
130	20131003	opera
1	20131003	safari
3	20131003	unknown
395	20131004	chrome
33	20131004	edge
7	20131004	firefox
69	20131004	opera
2	20131004	safari
1	20131004	unknown
-----------------------