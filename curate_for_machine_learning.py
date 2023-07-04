import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_trusted
Customer_trusted_node1688042068184 = glueContext.create_dynamic_frame.from_catalog(
    database="adozrl-stedi",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted_node1688042068184",
)

# Script generated for node step_trainer
step_trainer_node1688041572731 = glueContext.create_dynamic_frame.from_catalog(
    database="adozrl-stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_node1688041572731",
)

# Script generated for node Accelerometer
Accelerometer_node1688042961422 = glueContext.create_dynamic_frame.from_catalog(
    database="adozrl-stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometer_node1688042961422",
)

# Script generated for node Join_step_w_customer
Join_step_w_customer_node1688042023653 = Join.apply(
    frame1=step_trainer_node1688041572731,
    frame2=Customer_trusted_node1688042068184,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_step_w_customer_node1688042023653",
)

# Script generated for node Join_w_acc
Join_w_acc_node1688043008806 = Join.apply(
    frame1=Join_step_w_customer_node1688042023653,
    frame2=Accelerometer_node1688042961422,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_w_acc_node1688043008806",
)

# Script generated for node Test_curated
Test_curated_node1688042408695 = glueContext.write_dynamic_frame.from_catalog(
    frame=Join_w_acc_node1688043008806,
    database="adozrl-stedi",
    table_name="machine_learning_curated",
    transformation_ctx="Test_curated_node1688042408695",
)

job.commit()
