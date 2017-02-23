// This scala script is meant to be executed in spark-shell as a quick proof of concept
// 
// Requirement: Spark 2.0+

import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}

// define schema for raw log
val logSchema =  StructType(Array(
StructField("timestamp",TimestampType,true), 
StructField("elb",StringType,true), 
StructField("client_port",StringType,true), 
StructField("backend_port",StringType,true), 
StructField("request_processing_time",StringType,true), 
StructField("backend_processing_time",StringType,true), 
StructField("response_processing_time",StringType,true), 
StructField("elb_status_code",StringType,true), 
StructField("backend_status_code",StringType,true), 
StructField("received_bytes",StringType,true), 
StructField("sent_bytes",StringType,true), 
StructField("request",StringType,true), 
StructField("user_agent",StringType,true), 
StructField("ssl_cipher",StringType,true), 
StructField("ssl_protocol",StringType,true)))

// load compressed log as csv
val source = spark.read.format("csv").option("header", "false").option("delimiter", " ").schema(logSchema).load("2015_07_22_mktplace_shop_web_log_sample.log.gz")

// take slice of raw data we are interested in
val source_t1 = source.map(r => (r.getString(2).split(":")(0), r.getTimestamp(0), r.getString(11)))
source_t1.toDF.createOrReplaceTempView("source_t1")

// save processed data into parquet
spark.sql("""
SELECT _1 as client, _2 as timestamp, _3 as request, CONCAT(_1, CONCAT('_', SUM(new_session) OVER (PARTITION BY _1 ORDER BY _2))) AS session_id 
FROM (SELECT *, CASE WHEN UNIX_TIMESTAMP(_2) - LAG (UNIX_TIMESTAMP(_2)) OVER (PARTITION BY _1 ORDER BY _2) >= 15 * 60 THEN 1 ELSE 0 END AS new_session FROM source_t1)
""").write.parquet("source_prepared")

// load the parquet file
val source_prepared = spark.read.parquet("source_prepared")

source_prepared.toDF.createOrReplaceTempView("source_prepared")

// Q1 Determine the average session time
val q1 = spark.sql("""
select avg(duration) from (select max(unix_timestamp(timestamp)) - min(unix_timestamp(timestamp)) as duration 
from source_prepared 
group by session_id)
""")

q1.show()

// Answer
// +------------------+
// |     avg(duration)|
// +------------------+
// |100.69901029402477|
// +------------------+

// Q2 Determine unique URL visits per session
val q2 = spark.sql("""
select session_id, count(distinct request) from source_prepared group by session_id
""")

q2.show()

// Optionally save to csv, parquet etc for futher analysis
// sq2.write.csv("unique_url_per_session")

// Answer
// +-----------------+-----------------------+
// |       session_id|count(DISTINCT request)|
// +-----------------+-----------------------+
// |103.241.227.181_0|                     22|
// |205.175.226.101_0|                     89|
// |    1.39.15.172_1|                     57|
// |     136.8.2.68_3|                    108|
// |  101.57.193.44_0|                     82|
// |  14.99.154.110_0|                     23|
// |  185.66.155.61_0|                     44|
// | 117.200.16.215_0|                      4|
// | 117.194.98.174_0|                     15|
// | 106.194.42.184_0|                      6|
// |115.250.105.126_0|                     26|
// |   106.77.121.1_0|                      4|
// |  182.68.136.65_0|                    104|
// |    115.248.1.1_0|                     49|
// |  112.79.39.131_1|                     15|
// |    14.97.105.8_0|                     19|
// | 219.91.191.151_0|                      2|
// |   111.91.82.93_1|                      3|
// |  61.16.172.250_0|                      8|
// |    8.37.228.47_1|                     69|
// +-----------------+-----------------------+
// only showing top 20 rows

// Q3 Find the most engaged users, ie the IPs with the longest session times

val q3 = spark.sql("""
select split(session_id, '_')[0] as ip, max(unix_timestamp(timestamp)) - min(unix_timestamp(timestamp)) as duration 
from source_prepared 
group by session_id 
order by duration desc
""")

q3.show()

// sq3.write.csv("most_engaged_users")

// Answer
// +-----------------+--------+
// |       session_id|duration|
// +-----------------+--------+
// |   52.74.219.71_4|    2069|
// |  106.186.23.95_4|    2069|
// |  119.81.61.166_4|    2069|
// |   125.19.44.66_4|    2068|
// |   125.20.39.66_3|    2068|
// |  54.251.151.39_4|    2067|
// | 180.211.69.209_3|    2067|
// |   192.8.190.10_2|    2067|
// | 180.179.213.70_4|    2066|
// |  122.15.156.64_2|    2066|
// | 203.191.34.178_1|    2066|
// | 203.189.176.14_4|    2066|
// | 103.29.159.138_0|    2065|
// | 125.16.218.194_0|    2065|
// | 180.151.80.140_3|    2065|
// |213.239.204.204_1|    2065|
// |    78.46.60.71_0|    2064|
// | 103.29.159.186_1|    2064|
// | 103.29.159.213_1|    2063|
// |  192.71.175.30_2|    2063|
// +-----------------+--------+
// only showing top 20 rows










