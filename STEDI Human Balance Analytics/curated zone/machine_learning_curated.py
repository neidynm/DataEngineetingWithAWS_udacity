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

# Script generated for node accelerometer trusted
accelerometertrusted_node1727439540944 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1727439540944")

# Script generated for node step trainer trusted
steptrainertrusted_node1727439224272 = glueContext.create_dynamic_frame.from_catalog(database="neidy", table_name="step_trainer_landing", transformation_ctx="steptrainertrusted_node1727439224272")

# Script generated for node customer_curated
customer_curated_node1727439407425 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://neidy-lake-house/customer/trusted/run-1725745273017-part-r-00000"], "recurse": True}, transformation_ctx="customer_curated_node1727439407425")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * FROM (select * from step_trainer st where exists (select 1 from customer_curated c where c.serialnumber = st.serialnumber) ) s 
left join accelerometer_trusted a on a.timestamp = s.sensorReadingtime 
left join customer_curated c ON c.serialNumber = s.serialnumber
'''
SQLQuery_node1727439570318 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":steptrainertrusted_node1727439224272, "customer_curated":customer_curated_node1727439407425, "accelerometer_trusted":accelerometertrusted_node1727439540944}, transformation_ctx = "SQLQuery_node1727439570318")

# Script generated for node Amazon S3
AmazonS3_node1727440419898 = glueContext.getSink(path="s3://neidy-lake-house/machine-learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1727440419898")
AmazonS3_node1727440419898.setCatalogInfo(catalogDatabase="neidy",catalogTableName="machine_learning")
AmazonS3_node1727440419898.setFormat("json")
AmazonS3_node1727440419898.writeFrame(SQLQuery_node1727439570318)
job.commit()