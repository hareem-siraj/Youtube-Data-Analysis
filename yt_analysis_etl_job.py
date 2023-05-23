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

# Script generated for node Amazon S3
AmazonS3_node1684835899875 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://youtube-analysis-devv/youtube analysis project/regions/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1684835899875",
)

# Script generated for node Change Schema
ChangeSchema_node1684835903024 = ApplyMapping.apply(
    frame=AmazonS3_node1684835899875,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "string", "category_id", "string"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "string", "views", "string"),
        ("likes", "string", "likes", "string"),
        ("dislikes", "string", "dislikes", "string"),
        ("comment_count", "string", "comment_count", "string"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "string", "comments_disabled", "string"),
        ("ratings_disabled", "string", "ratings_disabled", "string"),
        ("video_error_or_removed", "string", "video_error_or_removed", "string"),
        ("description", "string", "description", "string"),
    ],
    transformation_ctx="ChangeSchema_node1684835903024",
)

# Script generated for node Amazon S3
AmazonS3_node1684835906757 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1684835903024,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://youtube-analysis-cleansed-data-devv/youtube analysis project/regions/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1684835906757",
)

job.commit()
