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

# Script generated for node Step_data_landing
Step_data_landing_node1688039075980 = glueContext.create_dynamic_frame.from_catalog(
    database="adozrl-stedi",
    table_name="step_trainer_landing",
    transformation_ctx="Step_data_landing_node1688039075980",
)

# Script generated for node Customers_trusted
Customers_trusted_node1688039730292 = glueContext.create_dynamic_frame.from_catalog(
    database="adozrl-stedi",
    table_name="customer_trusted",
    transformation_ctx="Customers_trusted_node1688039730292",
)

# Script generated for node Join
Step_data_landing_node1688039075980DF = Step_data_landing_node1688039075980.toDF()
Customers_trusted_node1688039730292DF = Customers_trusted_node1688039730292.toDF()
Join_node1688039814673 = DynamicFrame.fromDF(
    Step_data_landing_node1688039075980DF.join(
        Customers_trusted_node1688039730292DF,
        (
            Step_data_landing_node1688039075980DF["serialnumber"]
            == Customers_trusted_node1688039730292DF["serialnumber"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1688039814673",
)

# Script generated for node Step_data_trusted
Step_data_trusted_node1688039767161 = glueContext.write_dynamic_frame.from_catalog(
    frame=Join_node1688039814673,
    database="adozrl-stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Step_data_trusted_node1688039767161",
)

job.commit()
