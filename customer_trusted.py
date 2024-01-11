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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer landing
Customerlanding_node1704394588913 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customerlanding_node1704394588913",
)

# Script generated for node SQL Query
SqlQuery454 = """
select * from myDataSource
where shareWithResearchAsOfDate
is not null

"""
SQLQuery_node1704869101941 = sparkSqlQuery(
    glueContext,
    query=SqlQuery454,
    mapping={"myDataSource": Customerlanding_node1704394588913},
    transformation_ctx="SQLQuery_node1704869101941",
)

# Script generated for node Customer trusted
Customertrusted_node1704394616970 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1704869101941,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-house/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="Customertrusted_node1704394616970",
)

job.commit()
