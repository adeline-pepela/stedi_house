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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1704567606204 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlanding_node1704567606204",
)

# Script generated for node Customer trusted
Customertrusted_node1704567620054 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1704567620054",
)

# Script generated for node Join
Join_node1704567625166 = Join.apply(
    frame1=Accelerometerlanding_node1704567606204,
    frame2=Customertrusted_node1704567620054,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1704567625166",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1704567638335 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1704567625166,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-house/accelerometer/trusted/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometertrusted_node1704567638335",
)

job.commit()
