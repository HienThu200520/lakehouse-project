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

# Define your transformation logic here
# Example: Read data from customer_landing table, filter records based on shareWithResearchAsOfDate, and store in customer_trusted table

datasource = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "customer_landing")
filtered_data = Filter.apply(frame = datasource, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

glueContext.write_dynamic_frame.from_catalog(frame = filtered_data, database = "my_database", table_name = "customer_trusted")

job.commit()
