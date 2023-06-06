CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_trusted (
  timeStamp BIGINT,
  user STRING,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://balalal/accelerometer_trusted/'