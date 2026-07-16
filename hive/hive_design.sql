-- Database for Telecom Analytics
CREATE DATABASE telecom;

-- External table for real-time congestion analytics
CREATE EXTERNAL TABLE tower_congestion_rt (
  tower_id STRING,
  total_calls BIGINT,
  status STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP
)
PARTITIONED BY (p_date DATE)
STORED AS PARQUET
LOCATION '/user/telecom/hive/tower_congestion_rt';

