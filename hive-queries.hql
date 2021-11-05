-- you can change DB
USE default;

CREATE TABLE IF NOT EXISTS logs (
user_id string,
domain_log string,
authentication string,
time string,
request_method string,
uri string,
protocol string,
response_code string,
size int)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

-- check check
SHOW TABLES;
DESC logs;

-- loding from HDFS
LOAD DATA INPATH '/user/cloudera/logs_ready/part-m-00000' INTO TABLE logs;

-- check check
SELECT * FROM logs LIMIT 1;

/*Multiple analysis queries:*/

-- total num of requests to the server from each client
select count(uri) as cnt, user_id
from logs
group by user_id
cluster by cnt;

-- total success response (http=200) per request (a request might be occured just once!)
select user_id, count(response_code) as total_successful_response from logs
where response_code = 200
group by user_id
cluster by total_successful_response;

-- num of hits per uri + total data size rounded in KBs
select uri, count(uri) as uri_total_hits, round(size / 1024) as data_size_in_KB from logs
group by uri, size
order by uri_total_hits desc;