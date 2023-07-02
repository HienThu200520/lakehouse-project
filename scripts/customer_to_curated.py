import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'])

# Read customer_trusted data
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                     table_name="customer_trusted")

# Read accelerometer_trusted data
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                           table_name="accelerometer_trusted")

# Perform inner join on a suitable field (e.g., timestamp) to join customer_trusted and accelerometer_trusted data
joined_dyf = Join.apply(customer_trusted_dyf, accelerometer_trusted_dyf, 'join_field', 'join_field')

# Write to curated table
glueContext.write_dynamic_frame.from_catalog(frame=joined_dyf,
                                             database="my_database",
                                             table_name="curated_table")

job.commit()
