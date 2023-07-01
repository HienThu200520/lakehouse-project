CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  -- Define the schema fields and data types based on your JSON structure
  -- Example:
  serialnumber STRING,
  name STRING,
  email STRING,
  shareWithResearchAsOfDate DATE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://my_bucket/customer_landing/'
;
