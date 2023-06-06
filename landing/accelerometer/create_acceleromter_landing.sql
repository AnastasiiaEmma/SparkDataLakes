CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  timeStamp STRING COMMENT 'from deserializer',
  user STRING COMMENT 'from deserializer',
  x DOUBLE COMMENT 'from deserializer',
  y DOUBLE COMMENT 'from deserializer',
  z DOUBLE COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'dots.in.keys'='FALSE',
  'ignore.malformed.json'='FALSE',
  'mapping'='TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://balalal/accelerometer_landing/'