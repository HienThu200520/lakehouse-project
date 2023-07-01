CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  -- Define the schema fields and data types based on your JSON structure
  -- Example:
  serialnumber STRING,
  timestamp STRING,
  x FLOAT,
  y FLOAT,
  z FLOAT,
  shareWithResearchAsOfDate DATE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://my_bucket/accelerometer_landing/'
;
