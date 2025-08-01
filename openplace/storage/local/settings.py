from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy.engine import Engine

import os
from typing import Tuple

import logging

logger = logging.getLogger(__name__)

def connect_to_db(echo: bool = False) -> Tuple[Engine, Session]:
    """
    Connect to the local SQLite database using a path from the LOCAL_DB_PATH environment variable if set.

    Returns:
        Tuple[Engine, Session]: A tuple containing the SQLAlchemy engine and session.
    """
    db_path = os.environ.get("LOCAL_DB_PATH", "openplace.db")
    logger.info(f"Connecting to SQLite database at path: {db_path}")
    try:
        engine = create_engine(f"sqlite:///{db_path}", echo=echo)
        session = Session(engine)
        logger.info("Successfully connected to the SQLite database.")
        return engine, session
    except Exception as e:
        logger.error(f"Failed to connect to the SQLite database at {db_path}: {e}", exc_info=True)
        raise

def create_tables(engine: Engine):
    """
    Create all tables in the database.
    """
    try:
        SQLModel.metadata.create_all(engine)
        # TODO: add a check to see if the tables already exist, avoid logging a create if they exist
        logger.info("Successfully created all tables in the SQLite database.")
    except Exception as e:
        logger.error(f"Failed to create all tables in the SQLite database: {e}", exc_info=True)
        raise