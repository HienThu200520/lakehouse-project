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

# Define your Glue Studio job logic here for populating the step_trainer_trusted table

# Example: Read the Step Trainer IoT data from S3, apply necessary transformations, and store in step_trainer_trusted table

datasource = glueContext.create_dynamic_frame.from_catalog(database = "my_database", table_name = "step_trainer_landing")
filtered_data = Filter.apply(frame = datasource, f = lambda x: x["shareWithResearchAsOfDate"] is not None)

glueContext.write_dynamic_frame.from_catalog(frame = filtered_data, database = "my_database", table_name = "step_trainer_trusted")

job.commit()
