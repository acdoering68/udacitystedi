import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1688471125952 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_landing_node1688471125952",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="adostedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join
CustomerTrusted_node1DF = CustomerTrusted_node1.toDF()
Accelerometer_landing_node1688471125952DF = (
    Accelerometer_landing_node1688471125952.toDF()
)
Join_node1688471158162 = DynamicFrame.fromDF(
    CustomerTrusted_node1DF.join(
        Accelerometer_landing_node1688471125952DF,
        (
            CustomerTrusted_node1DF["email"]
            == Accelerometer_landing_node1688471125952DF["user"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1688471158162",
)

# Script generated for node Drop fields
Dropfields_node1688473094167 = ApplyMapping.apply(
    frame=Join_node1688471158162,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
    ],
    transformation_ctx="Dropfields_node1688473094167",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1688472596024 = DynamicFrame.fromDF(
    Dropfields_node1688473094167.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1688472596024",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1688472596024,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://adozrl4udacitystedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
