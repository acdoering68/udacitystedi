import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_landing
Customer_landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="customer_landing",
    transformation_ctx="Customer_landing_node1",
)

# Script generated for node Accelerator_landing
Accelerator_landing_node1688374576302 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerator_landing_node1688374576302",
)

# Script generated for node Filter
Filter_node1688372346545 = Filter.apply(
    frame=Customer_landing_node1,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="Filter_node1688372346545",
)

# Script generated for node Join
Filter_node1688372346545DF = Filter_node1688372346545.toDF()
Accelerator_landing_node1688374576302DF = Accelerator_landing_node1688374576302.toDF()
Join_node1688374540582 = DynamicFrame.fromDF(
    Filter_node1688372346545DF.join(
        Accelerator_landing_node1688374576302DF,
        (
            Filter_node1688372346545DF["email"]
            == Accelerator_landing_node1688374576302DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1688374540582",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1688374540582,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://adozrl4udacitystedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
