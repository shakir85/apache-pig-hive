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
