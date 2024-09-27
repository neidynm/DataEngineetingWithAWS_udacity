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

# Script generated for node Customer Trusted
CustomerTrusted_node1725813023621 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1725813023621")

# Script generated for node Step trainer landing
Steptrainerlanding_node1725813326665 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="step_trainer_landing", transformation_ctx="Steptrainerlanding_node1725813326665")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step_trainer s where exists (select 1 from customer_trusted c where c.serialnumber = s.serialnumber)
'''
SQLQuery_node1725813338014 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":Steptrainerlanding_node1725813326665, "customer_trusted":CustomerTrusted_node1725813023621}, transformation_ctx = "SQLQuery_node1725813338014")

# Script generated for node Amazon S3
AmazonS3_node1725814109651 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1725813338014, connection_type="s3", format="json", connection_options={"path": "s3://neidy-lake-house/step_trainer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1725814109651")

job.commit()