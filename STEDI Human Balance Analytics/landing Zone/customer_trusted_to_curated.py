import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Data
AccelerometerData_node1725740940778 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="accelerometer_trusted", transformation_ctx="AccelerometerData_node1725740940778")

# Script generated for node Customer trusted
Customertrusted_node1725740884315 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="customer_trusted", transformation_ctx="Customertrusted_node1725740884315")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct c.* from customer_trusted c INNER join accelerometer_trusted a
                  ON  a.user = c.email
'''
SQLQuery_node1725743667622 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":AccelerometerData_node1725740940778, "customer_trusted":Customertrusted_node1725740884315}, transformation_ctx = "SQLQuery_node1725743667622")

# Script generated for node Amazon S3
AmazonS3_node1725741127915 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1725743667622, connection_type="s3", format="json", connection_options={"path": "s3://neidy-lake-house/customer/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1725741127915")

job.commit()