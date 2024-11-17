import os

import pandas as pd
from sqlalchemy.types import Integer
from sqlalchemy import create_engine, Engine


def transform(df: pd.DataFrame) -> pd.DataFrame:

    # Rename Customer ID column to CustomerID for naming convention
    df = df.rename(columns={"Customer ID": "CustomerID"})

    # Cast InvoiceDate as datetime
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # Remove non-numeric CustomerID records. (Records with CustomerID=TEST)
    # Keep NaN CustomerID records as they are
    customer_id_to_numeric = pd.to_numeric(df["CustomerID"], errors="coerce")
    df = df[~customer_id_to_numeric.isna() | df["CustomerID"].isna()]

    return df


def get_database_credentials(env_var_prefix="MSSQL"):

    return {
        item: os.environ[f"{env_var_prefix}_{item}"]
        for item in ("USER", "PASSWORD", "HOST", "DATABASE")
    }


def get_database_engine(credentials: dict):

    user = credentials["USER"]
    password = credentials["PASSWORD"]
    host = credentials["HOST"]
    database = credentials["DATABASE"]

    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:1433/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )


def create_schema_if_not_exists(schema: str, engine: Engine):
    create_schema_if_not_exists_query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA [{schema}]');
        END
    """

    with engine.connect() as conn:
        _ = conn.exec_driver_sql(create_schema_if_not_exists_query)
        conn.commit()


def load_table(df: pd.DataFrame, table: str, schema: str, engine: Engine):

    df.to_sql(
        schema=schema,
        name=table,
        chunksize=20000,
        con=engine,
        if_exists="replace",
        index=False,
        dtype={"CustomerID": Integer},
    )
