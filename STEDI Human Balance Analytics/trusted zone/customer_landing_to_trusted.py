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

# Script generated for node Amazon S3
AmazonS3_node1723928040901 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://neidy-lake-house/customer/landing/customer-1691348231425.json"], "recurse": True}, transformation_ctx="AmazonS3_node1723928040901")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where sharewithresearchasofdate is not null
'''
SQLQuery_node1724015501957 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1723928040901}, transformation_ctx = "SQLQuery_node1724015501957")

# Script generated for node Amazon S3
AmazonS3_node1724011577397 = glueContext.getSink(path="s3://neidy-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724011577397")
AmazonS3_node1724011577397.setCatalogInfo(catalogDatabase="neidy",catalogTableName="customer_trusted")
AmazonS3_node1724011577397.setFormat("json")
AmazonS3_node1724011577397.writeFrame(SQLQuery_node1724015501957)
job.commit()
