# HiveTestDataGenerator

Hive Test Data Generator

This is Hadoop MapReduce application that generates test data for Hive external table, it reads Apache Hive's table creation query and depending on its DDL, it generates row data in TEXTFILE format.

I referred to https://issues.apache.org/jira/browse/HIVE-8792


BUILD
mvn clean package


USAGE

Puts your DDL query on HDFS or S3 and specify as -q option
e.g)
hadoop jar ./HiveTestDataGenerator-1.0-SNAPSHOT.jar -q s3n://path/creation_query.sql -p 3 -c 3 -r 11


usage: hiveTestDataGenerator
 -c,--file-count-per-partition <arg>   Output file count per partition,
                                       for no partition, total number of
                                       output files
 -d,--destination-path <arg>           Target path to generate test data
 -f,--file-format <arg>                Output file format
 -p,--partition-count <arg>            Number of partitions to generate
 -q,--query-path                       Hive creation DDL file on S3
 -r,--row-count-per-file <arg>         the number of row per each input
                                       file to generate
                                       

example 1) For partitioned table

To create 4 partitions, 3 files per partition, 100 rows per file



hdfs dfs -cat s3n://path/creation_query.sql

CREATE EXTERNAL TABLE table_name (
symbol string,
ymd DATE,
price_open double,
price_end DECIMAL(10),
price_low VARCHAR(3),
price_high DECIMAL(18,8),
time TIMESTAMP,
volume bigint)
PARTITIONED BY (`partition-key` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION "s3n://your-bucket/table/table-name/";


hadoop jar ./HiveTestDataGenerator-1.0-SNAPSHOT.jar -q s3n://path/creation_query.sql -p 3 -c 3 -r 100
The above command creates the following files

For partition 1
s3n://your-bucket/table/table-name/partition-key=random-value1/partition-r-00000
s3n://your-bucket/table/table-name/partition-key=random-value1/partition1-r-000000
s3n://your-bucket/table/table-name/partition-key=random-value1/partition2-r-000000

For partition 2
s3n://your-bucket/table/table-name/partition-key=random-value2/partition-r-00000
s3n://your-bucket/table/table-name/partition-key=random-value2/partition1-r-000000
s3n://your-bucket/table/table-name/partition-key=random-value2/partition2-r-000000

For partition 3
s3n://your-bucket/table/table-name/partition-key=random-value3/partition-r-00000
s3n://your-bucket/table/table-name/partition-key=random-value3/partition1-r-000000
s3n://your-bucket/table/table-name/partition-key=random-value3/partition2-r-000000

For partition 4
s3n://your-bucket/table/table-name/partition-key=random-value4/partition-r-00000
s3n://your-bucket/table/table-name/partition-key=random-value4/partition1-r-000000
s3n://your-bucket/table/table-name/partition-key=random-value4/partition2-r-000000


example 2) For non-partitioned table

hadoop jar ./HiveTestDataGenerator-1.0-SNAPSHOT.jar -q s3n://path/creation_query.sql -p 3 -c 3 -r 100

When table is non-partitioned table "-p 3" option will be ignored and 3 files will be generated under table's location with 100 rows


s3n://your-bucket/table/table-name/output-r-000000
s3n://your-bucket/table/table-name/output1-r-000000
s3n://your-bucket/table/table-name/output2-r-000000


Unsupported feature yet

Data type:
Complex types are not supported yet.
Complex Types (https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-ComplexTypes)

ARRAY
MAP
STRUCT
UNIONTYPE

Only supports TEXTFILE format