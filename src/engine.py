"""Database engine builder (Presto / Hive / SQLAlchemy)"""

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import urllib3
from typing import Optional

load_dotenv()


def build_presto_engine(use_env: bool = True, connection_override: Optional[str] = None) -> Engine:
    """
    Build and return a SQLAlchemy engine for Presto (or other supported DBs).
    Expects credentials in environment variables. If connection_override is provided it will be used directly
    as the SQLAlchemy URL.

    Environment variables used (example names):
      - HIVE_SVC_USER
      - HIVE_SVC_PASS
      - HIVE_SVC_ADDRESS
      - HIVE_SVC_PORT
      - HIVE_SVC_DBNAME
      - HIVE_SVC_SCHEMA

    Returns:
        SQLAlchemy Engine
    """
    # allow direct override for testing
    if connection_override:
        engine = create_engine(connection_override)
        return engine

    username = os.getenv("HIVE_SVC_USER")
    password = os.getenv("HIVE_SVC_PASS")
    address = os.getenv("HIVE_SVC_ADDRESS")
    port = os.getenv("HIVE_SVC_PORT")
    dbname = os.getenv("HIVE_SVC_DBNAME")
    schema = os.getenv("HIVE_SVC_SCHEMA")

    if not all([username, password, address, port, dbname, schema]):
        raise EnvironmentError(
            "Missing one or more DB connection environment variables. "
            "Check HIVE_SVC_USER, HIVE_SVC_PASS, HIVE_SVC_ADDRESS, HIVE_SVC_PORT, HIVE_SVC_DBNAME, HIVE_SVC_SCHEMA."
        )

    # Example Presto connection string format:
    # presto://<user>:<password>@<host>:<port>/<catalog>/<schema>
    sql_url = f"presto://{username}:{password}@{address}:{port}/{dbname}/{schema}"

    # disable insecure warnings if verify disabled by driver requirements
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    engine = create_engine(
        sql_url,
        connect_args={"protocol": "https", "requests_kwargs": {"verify": False}},
        pool_pre_ping=True,
    )
    return engine
