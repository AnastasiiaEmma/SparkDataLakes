import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Create a SparkContext, a Gluecontext, a Glue job and initialize the job with arg  
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Generate a script for node S3 steptrainer landing
S3steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://balalal/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3step_trainer_landing_node1",
)

# Generate a script for node S3 customer trusted
S3customertrusted_node1681397113778 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://balalal/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3customertrusted_node1681397113778",
)

# Generate a script for node Rename Customer Columns
RenameCustomerColumns_node1681397537375 = ApplyMapping.apply(
    frame=S3customertrusted_node1681397113778,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "`(right) shareWithPublicAsOfDate`",
            "bigint",
        ),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        ("registrationDate", "bigint", "`(right) registrationDate`", "bigint"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "`(right) shareWithResearchAsOfDate`",
            "bigint",
        ),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "bigint", "`(right) lastUpdateDate`", "bigint"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "`(right) shareWithFriendsAsOfDate`",
            "bigint",
        ),
    ],
    transformation_ctx="RenameCustomerColumns_node1681397537375",
)

# Generate a script for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=S3steptrainerlanding_node1,
    frame2=RenameCustomerColumns_node1681397537375,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="JoinCustomer_node2",
)

# Generate a script for node Drop Fields
DropFields_node1681397335479 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "`(right) serialNumber`",
        "`(right) shareWithPublicAsOfDate`",
        "`(right) birthDay`",
        "`(right) registrationDate`",
        "`(right) shareWithResearchAsOfDate`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) lastUpdateDate`",
        "`(right) phone`",
        "`(right) shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1681397335479",
)

# Generate a script for node S3 steptrainer trusted
S3steptrainertrusted_node3 = glueContext.getSink(
    path="s3://balalal/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3steptrainertrusted_node3",
)
S3steptrainertrusted_node3.setCatalogInfo(
    catalogDatabase="landing", catalogTableName="steptrainer_trusted"
)
S3steptrainertrusted_node3.setFormat("json")
S3steptrainertrusted_node3.writeFrame(DropFields_node1681397335479)
job.commit()