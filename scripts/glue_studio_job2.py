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
job.init(args['JOB_NAME'], args)

# Define your Glue Studio job logic here for creating the machine_learning_curated table

# Example: Read data from customer_trusted and accelerometer_trusted tables, apply necessary joins and filters, and store in machine_learning_curated table

customer_data = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "customer_trusted")
accelerometer_data = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "accelerometer_trusted")

# Perform required joins and filters to create the aggregated table

glueContext.write_dynamic_frame.from_catalog(frame = curated_data, database = "my_database", table_name = "machine_learning_curated")

job.commit()
