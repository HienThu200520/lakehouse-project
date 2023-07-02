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

# Read accelerometer_landing data
accelerometer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                          table_name="accelerometer_landing")

# Read customer_trusted data
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(database="my_database",
                                                                      table_name="customer_trusted")

# Perform inner join on serialnumber field
joined_dyf = Join.apply(accelerometer_landing_dyf, customer_trusted_dyf, 'serialnumber', 'serialnumber')

# Filter records with non-blank shareWithResearchAsOfDate
filtered_dyf = Filter.apply(frame=joined_dyf,
                            f=lambda x: x["shareWithResearchAsOfDate"] is not None and x["shareWithResearchAsOfDate"] != "")

# Write to accelerometer_trusted table
glueContext.write_dynamic_frame.from_catalog(frame=filtered_dyf,
                                             database="my_database",
                                             table_name="accelerometer_trusted")

job.commit()
