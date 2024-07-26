import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_file_landing = f's3://{V.DATA_LANDING_BUCKET_NAME}/orcs_test/ai4e_movie_test.csv'

df = spark.read.option('header', 'true').option('delimiter', ',').csv(s3_file_landing)

s3_saving_path = f's3://{V.DATA_LANDING_BUCKET_NAME}/golden_zone/demo_table'

df.coalesce(1).write.mode('overwrite').parquet(s3_saving_path)
job.commit()
