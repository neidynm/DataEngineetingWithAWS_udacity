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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1725738960667 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1725738960667")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1725738961234 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1725738961234")

# Script generated for node Join
Join_node1725739006052 = Join.apply(frame1=AWSGlueDataCatalog_node1725738960667, frame2=AWSGlueDataCatalog_node1725738961234, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1725739006052")

# Script generated for node Select Fields
SelectFields_node1725739662017 = SelectFields.apply(frame=Join_node1725739006052, paths=["z", "user", "x", "y", "timestamp"], transformation_ctx="SelectFields_node1725739662017")

# Script generated for node Amazon S3
AmazonS3_node1725739724531 = glueContext.getSink(path="s3://neidy-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725739724531")
AmazonS3_node1725739724531.setCatalogInfo(catalogDatabase="neidy",catalogTableName="accelerometer_trusted")
AmazonS3_node1725739724531.setFormat("glueparquet", compression="snappy")
AmazonS3_node1725739724531.writeFrame(SelectFields_node1725739662017)
job.commit()