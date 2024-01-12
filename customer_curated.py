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

# Script generated for node accelerometer trusted
accelerometertrusted_node1705040218860 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/accelerometer/trusted1/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1705040218860",
)

# Script generated for node Customer trusted
Customertrusted_node1704568918476 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1704568918476",
)

# Script generated for node Join
Join_node1705040241477 = Join.apply(
    frame1=accelerometertrusted_node1705040218860,
    frame2=Customertrusted_node1704568918476,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705040241477",
)

# Script generated for node Drop Fields
DropFields_node1705040271143 = DropFields.apply(
    frame=Join_node1705040241477,
    paths=[
        "serialNumber",
        "`.customerName`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "`.email`",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "`.phone`",
        "`.shareWithPublicAsOfDate`",
        "shareWithFriendsAsOfDate",
        "`.birthDay`",
        "`.lastUpdateDate`",
        "`.shareWithFriendsAsOfDate`",
        "timestamp",
        "`.registrationDate`",
        "`.serialNumber`",
        "lastUpdateDate",
        "email",
        "`.shareWithResearchAsOfDate`",
        "phone",
    ],
    transformation_ctx="DropFields_node1705040271143",
)

# Script generated for node Customer curated
Customercurated_node1704568928823 = glueContext.getSink(
    path="s3://stedi-house/customer/curated2/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Customercurated_node1704568928823",
)
Customercurated_node1704568928823.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="customer_curated"
)
Customercurated_node1704568928823.setFormat("json")
Customercurated_node1704568928823.writeFrame(DropFields_node1705040271143)
job.commit()
