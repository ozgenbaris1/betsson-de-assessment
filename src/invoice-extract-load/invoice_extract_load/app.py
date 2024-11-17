import logging

import pandas as pd

from invoice_extract_load.utils import transform
from invoice_extract_load.utils import get_database_credentials
from invoice_extract_load.utils import get_database_engine
from invoice_extract_load.utils import create_schema_if_not_exists
from invoice_extract_load.utils import load_table

# Configure logging to include timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run(
    from_: str,
    encoding: str,
    target_schema: str,
    target_table: str,
):

    logger.info(f"Reading CSV file ({from_})")
    df = pd.read_csv(from_, encoding=encoding)

    logger.info(f"Running transformations on the DataFrame")
    df = transform(df=df)

    logger.info(f"Getting target database credentials")
    database_credentials = get_database_credentials(env_var_prefix="MSSQL")

    logger.info(f"Creating a database engine")
    engine = get_database_engine(credentials=database_credentials)

    logger.info(f"Creating schema {target_schema} if it doesn't exist")
    create_schema_if_not_exists(engine=engine, schema=target_schema)

    logger.info(f"Loading data to {target_schema}.{target_table}")
    load_table(engine=engine, df=df, schema=target_schema, table=target_table)

    logger.info(f"The pipeline completed successfully")
