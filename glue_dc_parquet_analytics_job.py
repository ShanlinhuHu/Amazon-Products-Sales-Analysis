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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1693478380960 = glueContext.create_dynamic_frame.from_catalog(
    database="amazon-products-sales-analysis-clean",
    table_name="amazon_products_sales_analysis_clean_useast2_dev",
    transformation_ctx="AWSGlueDataCatalog_node1693478380960",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://amazon-products-sales-analysis-analytics-useast2-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["main_category"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="amazon_products_sales_analysis_analytics",
    catalogTableName="amazon-products-sales-analysis-analytics-useast2-dev",
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(AWSGlueDataCatalog_node1693478380960)
job.commit()
