import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Apply filter transformation to remove rows with blank shareWithResearchAsOfDate
filtered_dynamic_frame = Filter.apply(
    frame=raw_dynamic_frame,
    f=lambda x: x["shareWithResearchAsOfDate"] is not None
)

# Convert the filtered dynamic frame back to a Glue dynamic frame
trusted_dynamic_frame = DynamicFrame.fromDF(
    filtered_dynamic_frame.toDF(),
    glueContext,
    "trusted_dynamic_frame"
)

# Write the trusted dynamic frame to the trusted zone
glueContext.write_dynamic_frame.from_catalog(
    frame=trusted_dynamic_frame,
    database="your_database_name",
    table_name="customer_trusted"
)

# Define your transformation logic here
# Example: Read data from customer_landing table, filter records based on shareWithResearchAsOfDate, and store in customer_trusted table

datasource = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "customer_landing")
filtered_data = Filter.apply(frame = datasource, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

glueContext.write_dynamic_frame.from_catalog(frame = filtered_data, database = "my_database", table_name = "customer_trusted")

job.commit()
