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

# Script generated for node step trainer
steptrainer_node1704569911559 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_node1704569911559",
)

# Script generated for node customer trusted
customertrusted_node1704569909720 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1704569909720",
)

# Script generated for node Join
Join_node1704569914657 = Join.apply(
    frame1=steptrainer_node1704569911559,
    frame2=customertrusted_node1704569909720,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1704569914657",
)

# Script generated for node step_trainer trusted
step_trainertrusted_node1704569918599 = glueContext.getSink(
    path="s3://stedi-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainertrusted_node1704569918599",
)
step_trainertrusted_node1704569918599.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="step_trainer trusted"
)
step_trainertrusted_node1704569918599.setFormat("json")
step_trainertrusted_node1704569918599.writeFrame(Join_node1704569914657)
job.commit()
