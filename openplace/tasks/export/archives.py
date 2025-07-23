import logging
import duckdb
from datetime import datetime
from typing import Optional

from duckdb import DuckDBPyConnection as Connection

logger = logging.getLogger(__name__)

def connect_to_database(db_path: str = "openplace.db") -> Connection:
    """
    Connect to the duckdb database.
    """
    con = duckdb.connect(":memory:")
    con.install_extension("sqlite")
    con.load_extension("sqlite")

    con.execute(f"""ATTACH '{db_path}' AS openplace (TYPE sqlite);""")
    con.execute("USE openplace;")
    return con

def sqlite_export(
    con: Connection,
    output_dir: str,
    table_name: str,
    output_format: str = "parquet",
    compression: str = "gzip",
    use_date_in_filename: bool = False,
) -> None:
    """
    Export the given table to the given directory.
    """
    date = datetime.now().strftime("%Y-%m-%d") if use_date_in_filename else ""
    filename = f"archives-{date}" if use_date_in_filename else "archives"
    match output_format:
        case "parquet":
            con.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_dir}/{filename}.parquet' (FORMAT 'parquet')")
            logger.info(f"Exported {table_name} to {output_dir}/{filename}.parquet")
        case "jsonl":
            con.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_dir}/{filename}.jsonl' (FORMAT 'jsonl', COMPRESSION '{compression}')")
            logger.info(f"Exported {table_name} to {output_dir}/{filename}.jsonl.gz")
        case "csv":
            con.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_dir}/{filename}.csv' (FORMAT 'csv', HEADER true, COMPRESSION '{compression}')")
            logger.info(f"Exported {table_name} to {output_dir}/{filename}.csv.gz")
        case _:
            raise ValueError(f"Invalid output format: {output_format}")

def export_archives(output_dir: str = ".", output_format: str = "parquet", compression: str = "gzip", use_date_in_filename: bool = False) -> str:
    """
    Export the archives from the database.
    If no output directory is provided, the archives will be exported to a file named "archives.parquet" in the current directory.
    If no output format is provided, the archives will be exported to a parquet file.

    Args:
        output_dir: The directory to export the archives to.
        output_format: The format to export the archives to.
        compression: The compression to use (only supported for jsonl and csv)
    Returns:
        The path to the exported file.
    """
    logger.info(f"Exporting archives to {output_dir} in {output_format} format")
    con = connect_to_database()
    sqlite_export(con, output_dir, "archivecontent", output_format, compression, use_date_in_filename)
    con.close()
    return output_dir