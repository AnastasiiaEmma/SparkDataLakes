CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customername STRING COMMENT 'from deserializer',
  email STRING COMMENT 'from deserializer',
  phone STRING COMMENT 'from deserializer',
  birthday STRING COMMENT 'from deserializer',
  serialnumber STRING COMMENT 'from deserializer',
  registrationdate BIGINT COMMENT 'from deserializer',
  lastupdatedate BIGINT COMMENT 'from deserializer',
  sharewithresearchasofdate BIGINT COMMENT 'from deserializer',
  sharewithpublicasofdate BIGINT COMMENT 'from deserializer',
  sharewithfriendsasofdate BIGINT COMMENT 'from deserializer'
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
LOCATION 's3://balalal/customer_landing/'