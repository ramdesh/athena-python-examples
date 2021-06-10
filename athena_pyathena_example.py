import logging.config
import time
from urllib.parse import quote_plus  # PY2: from urllib import quote_plus

import pandas as pd
from sqlalchemy.engine import create_engine

from .log_config import LOG_CONFIG

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("athena_python_examples")

AWS_ACCESS_KEY = "AWS_ACCESS_KEY"
AWS_SECRET_KEY = "AWS_SECRET_KEY"
SCHEMA_NAME = "schema_name"
S3_STAGING_DIR = "s3://s3-results-bucket/output/"
AWS_REGION = "us-east-1"


conn_str = (
    "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@"
    "athena.{region_name}.amazonaws.com:443/"
    "{schema_name}?s3_staging_dir={s3_staging_dir}&work_group=primary"
)


# Create the SQLAlchemy connection. Note that you need to have pyathena installed for this.
engine = create_engine(
    conn_str.format(
        aws_access_key_id=quote_plus(AWS_ACCESS_KEY),
        aws_secret_access_key=quote_plus(AWS_SECRET_KEY),
        region_name=AWS_REGION,
        schema_name=SCHEMA_NAME,
        s3_staging_dir=quote_plus(S3_STAGING_DIR),
    )
)
logger.info("Created athena connection")

conn = engine.connect()
start_time = time.time()
df_data = pd.read_sql_query("select * from table", conn)
logger.info(df_data.head())
logger.info(f"Data fetched in {time.time() - start_time}s")
