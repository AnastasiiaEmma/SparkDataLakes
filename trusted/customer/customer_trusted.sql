CREATE EXTERNAL TABLE IF NOT EXISTS customer_trusted (
  customerName STRING,
  email STRING,
  phone STRING,
  birthDay STRING,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://balalal/customer_trusted/'
