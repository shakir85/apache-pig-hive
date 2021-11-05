# Logs-Analysis

Simple Apache Pig script for log files processing. It reads local files, extracts data-fields, then it converts them to comma delimited file. The goal is to make this file ready to be queried from Apache Hive. The logs are Apache-Common formatted.

## Tools and Configurations
Apache Pig and Apache Hive in MapReduce mode using Cloudera sandbox. The input log file is stored in a local HDFS and the output is going to be exported to Hadoop. Data extraction is processed via `CommonLogLoader()` function retrieved from Pig source code [PiggyBank SVN] http://svn.apache.org/repos/asf/pig/trunk/ All required dependencies included in the `dependecies.xml` file

**Required Jar files **
- `hive-jdbc-x.x.x-standalone.jar` _(`-x.x.x` is the current Hive version)_
- `hive-jdbc-x.x.x.jar`
- `hive-metastore-x.x.x.jar`
- `hive-service-x.x.x.jar`
- `mysql-connector-java-5.0.8-bin.jar`
- `hadoop-core-1.2.1.jar`

## Final Schema

`| user_ip | domain_log  | authentication | time | request_method | uri | protocol | response_code | size |`



## Pig Data Extraction Job

```pig
-- Run Pig on MapReduce mode
--Register PiggyBnak to load log UDF
REGISTER /usr/lib/pig/piggybank.jar;

DEFINE CommonLogLoader org.apache.pig.piggybank.storage.apachelog.CommonLogLoader();

-- load from HDFS
logs_raw = LOAD '/user/cloudera/access_log'
            USING CommonLogLoader
            AS (user_ip, domain_log, authentication, time, request_method,
                uri, protocol, response_code, size);

-- store data to HDFS as CSV
STORE logs_raw INTO '/user/cloudera/logs_ready' USING PigStorage(',');
```

## HiveQL Queries

### Create Hive table

```hiveql
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
```

### Loading data into Hive table

```pig
-- loding from HDFS
LOAD DATA INPATH '/user/cloudera/logs_ready/part-m-00000' INTO TABLE logs;

-- check check
SELECT * FROM logs LIMIT 1;
```

Example Hive Queries:

1. Number of hits per uri + total data size in KBs

```hiveql
select uri, count(uri) as uri_total_hits, round(size / 1024) as data_size_in_KB from logs
group by uri, size
order by uri_total_hits desc;
```

2. Total number of requests to server from each client

```hiveql
select count(uri) as cnt, user_id
from logs
group by user_id
cluster by cnt;
```

3. Total success responses (http=200) per request:

```hiveql
select user_id, count(response_code) as total_successful_response from logs
where response_code = 200
group by user_id
cluster by total_successful_response;
```
