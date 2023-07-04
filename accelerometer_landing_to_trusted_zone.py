import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer_trusted
Customer_trusted_node1688458175858 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted_node1688458175858",
)

# Script generated for node Join
AccelerometerLanding_node1DF = AccelerometerLanding_node1.toDF()
Customer_trusted_node1688458175858DF = Customer_trusted_node1688458175858.toDF()
Join_node1688458090903 = DynamicFrame.fromDF(
    AccelerometerLanding_node1DF.join(
        Customer_trusted_node1688458175858DF,
        (
            AccelerometerLanding_node1DF["user"]
            == Customer_trusted_node1688458175858DF["email"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1688458090903",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1688458090903,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://adozrl4udacitystedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
