CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customer_id STRING,
  name STRING,
  email STRING,
  shareWithResearchAsOfDate DATE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://my-bucket/customer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Filter out rows with blank shareWithResearchAsOfDate
CREATE OR REPLACE VIEW customer_landing_filtered AS
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL;
