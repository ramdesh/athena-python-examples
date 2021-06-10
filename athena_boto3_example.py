import logging.config
import time
from typing import Dict

import boto3
import pandas as pd

from .log_config import LOG_CONFIG


logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("athena_python_examples")

AWS_ACCESS_KEY = "AWS_ACCESS_KEY"
AWS_SECRET_KEY = "AWS_SECRET_KEY"
SCHEMA_NAME = "schema_name"
S3_STAGING_DIR = "s3://s3-results-bucket/output/"
S3_BUCKET_NAME = "s3-results-bucket"
S3_OUTPUT_DIRECTORY = "output"
AWS_REGION = "us-east-1"


def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    logger.info(f"Time to complete query: {time.time() - start_time}s")
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

logger.info("Created athena connection")

start_time = time.time()
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM table",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
df_data = download_and_load_query_results(athena_client, response)
logger.info(df_data.head())
logger.info(f"Data fetched in {time.time() - start_time}s")
