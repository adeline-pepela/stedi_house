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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1704568920471 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometertrusted_node1704568920471",
)

# Script generated for node Customer trusted
Customertrusted_node1704568918476 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1704568918476",
)

# Script generated for node Join
Join_node1704568926302 = Join.apply(
    frame1=Accelerometertrusted_node1704568920471,
    frame2=Customertrusted_node1704568918476,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1704568926302",
)

# Script generated for node Drop Fields
DropFields_node1704569293529 = DropFields.apply(
    frame=Join_node1704568926302,
    paths=[
        "`.email`",
        "`.phone`",
        "email",
        "phone",
        "serialNumber",
        "`.customerName`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "`.shareWithPublicAsOfDate`",
        "shareWithFriendsAsOfDate",
        "`.birthDay`",
        "`.lastUpdateDate`",
        "`.shareWithFriendsAsOfDate`",
        "timestamp",
        "`.registrationDate`",
        "`.serialNumber`",
        "lastUpdateDate",
        "`.shareWithResearchAsOfDate`",
    ],
    transformation_ctx="DropFields_node1704569293529",
)

# Script generated for node Customer curated
Customercurated_node1704568928823 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704569293529,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customercurated_node1704568928823",
)

job.commit()
