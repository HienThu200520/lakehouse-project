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

# Read data from customer_landing table
customer_landing_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                                table_name="customer_landing")

# Read data from customer_trusted table
customer_trusted_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                                table_name="customer_trusted")

# Apply filter transformation to remove rows with blank shareWithResearchAsOfDate
filtered_dynamic_frame = Filter.apply(
    frame=raw_dynamic_frame,
    f=lambda x: x["shareWithResearchAsOfDate"] is not None
)

# Apply join transformation to join customer_landing and customer_trusted on the serialnumber field
joined_dynamic_frame = Join.apply(
    frame1=customer_landing_dynamic_frame,
    frame2=customer_trusted_dynamic_frame,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="joined_dynamic_frame"
)

# Convert the joined dynamic frame back to a Glue dynamic frame
trusted_dynamic_frame = DynamicFrame.fromDF(
    joined_dynamic_frame.toDF(),
    glueContext,
    "trusted_dynamic_frame"
)

# Write the trusted dynamic frame to the trusted zone
glueContext.write_dynamic_frame.from_catalog(
    frame=trusted_dynamic_frame,
    database="my_database",
    table_name="customer_trusted"
)

# Define your transformation logic here
# Example: Read data from customer_landing table, filter records based on shareWithResearchAsOfDate, and store in customer_trusted table

datasource = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "customer_landing")
filtered_data = Filter.apply(frame = datasource, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

glueContext.write_dynamic_frame.from_catalog(frame = filtered_data, database = "my_database", table_name = "customer_trusted")

job.commit()
