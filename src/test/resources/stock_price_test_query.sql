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
