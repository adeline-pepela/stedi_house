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

# Script generated for node step_trainer trusted
step_trainertrusted_node1704966850329 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainertrusted_node1704966850329",
)

# Script generated for node customer curated
customercurated_node1704966826190 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1704966826190",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1704966848539 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1704966848539",
)

# Script generated for node SQL Query
SqlQuery582 = """
select s.sensorreadingtime,s.distancefromobject from a join s on a.serialnumber=s.serialnumber
"""
SQLQuery_node1704970626190 = sparkSqlQuery(
    glueContext,
    query=SqlQuery582,
    mapping={
        "c": customercurated_node1704966826190,
        "a": accelerometertrusted_node1704966848539,
        "s": step_trainertrusted_node1704966850329,
    },
    transformation_ctx="SQLQuery_node1704970626190",
)

# Script generated for node machine_learning curated
machine_learningcurated_node1704966890378 = glueContext.getSink(
    path="s3://stedi-house/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learningcurated_node1704966890378",
)
machine_learningcurated_node1704966890378.setCatalogInfo(
    catalogDatabase="stedi-db", catalogTableName="machine_learning curated"
)
machine_learningcurated_node1704966890378.setFormat("json")
machine_learningcurated_node1704966890378.writeFrame(SQLQuery_node1704970626190)
job.commit()
