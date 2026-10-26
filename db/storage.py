
"""
storage.py
Lightweight SQLite persistence for past scan results.

Table: scan_results
    run_id              TEXT  — UUID for this scan run
    run_timestamp       TEXT  — ISO-8601 timestamp of the run
    dest_host           TEXT
    uri                 TEXT
    total_requests      INTEGER
    unique_clients      INTEGER
    est_sleep_sec       REAL
    timing_cv           REAL
    payload_cv          REAL
    domain_entropy      REAL
    composite_score     REAL
    window_coverage_pct REAL
    resp_consistency_pct REAL
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Create a logger to record information and errors from this module.
logger = logging.getLogger(__name__)

# Define the default location of the SQLite database file.
_DEFAULT_DB_PATH = Path(__file__).parent / "scan_results.db"

# SQL statement to create the scan_results table if it does not already exist.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS scan_results (
    run_id               TEXT,
    run_timestamp        TEXT,
    dest_host            TEXT,
    uri                  TEXT,
    total_requests       INTEGER,
    unique_clients       INTEGER,
    est_sleep_sec        REAL,
    timing_cv            REAL,
    payload_cv           REAL,
    domain_entropy       REAL,
    composite_score      REAL,
    window_coverage_pct  REAL,
    resp_consistency_pct REAL
);
"""

# List the columns containing scan result data, excluding run metadata.
_RESULT_COLUMNS = [
    "dest_host", "uri", "total_requests", "unique_clients",
    "est_sleep_sec", "timing_cv", "payload_cv", "domain_entropy",
    "composite_score", "window_coverage_pct", "resp_consistency_pct",
]


def _get_connection(db_path: Path) -> sqlite3.Connection:
    # Ensure the directory containing the database exists.
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Open a connection to the SQLite database.
    conn = sqlite3.connect(str(db_path))

    # Create the required table if it is missing.
    conn.execute(_CREATE_TABLE)
    conn.commit()

    return conn


def save_results(results_df: pd.DataFrame, db_path: Path = _DEFAULT_DB_PATH) -> str:
    """
    Persist *results_df* to SQLite with a new run_id.

    Parameters
    ----------
    results_df:
        Output of engine.detect_beacons().
    db_path:
        Path to the SQLite database file.  Created if absent.

    Returns
    -------
    str
        The run_id UUID string for this batch.
    """
    # Skip saving if the input DataFrame is missing or contains no rows.
    if results_df is None or results_df.empty:
        logger.info("No results to save.")
        return ""

    # Generate a unique identifier for this scan batch.
    run_id = str(uuid.uuid4())

    # Record the current time in UTC using ISO-8601 format.
    run_ts = datetime.now(timezone.utc).isoformat()

    # Prepare each scan result as a tuple matching the database column order.
    rows = []
    for _, row in results_df.iterrows():
        rows.append(
            (
                run_id,
                run_ts,
                str(row.get("dest_host", "")),
                str(row.get("uri", "")),
                int(row.get("total_requests", 0)),
                int(row.get("unique_clients", 0)),
                float(row.get("est_sleep_sec", 0.0)),
                float(row.get("timing_cv", 0.0)),
                float(row.get("payload_cv", 0.0)),
                float(row.get("domain_entropy", 0.0)),
                float(row.get("composite_score", 0.0)),
                float(row.get("window_coverage_pct", 0.0)),
                float(row.get("resp_consistency_pct", 0.0)),
            )
        )

    try:
        # Open the database connection and ensure the table exists.
        conn = _get_connection(db_path)

        # Insert all prepared rows using parameterized SQL placeholders.
        conn.executemany(
            """
            INSERT INTO scan_results VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )

        # Commit the transaction so the inserted records are saved.
        conn.commit()

        # Close the database connection after saving.
        conn.close()

        # Log the number of saved records and their shared run identifier.
        logger.info("Saved %d rows under run_id %s", len(rows), run_id)

    except Exception as exc:  # noqa: BLE001
        # Log any error that occurs while saving the scan results.
        logger.error("Failed to save results: %s", exc)

    # Return the identifier generated for this scan batch.
    return run_id


def load_history(db_path: Path = _DEFAULT_DB_PATH, limit: int = 500) -> pd.DataFrame:
    """
    Load the most recent *limit* rows from the scan history.

    Returns an empty DataFrame if the database does not exist yet.
    """
    # If no database exists, return an empty DataFrame with the expected columns.
    if not db_path.exists():
        return pd.DataFrame(columns=["run_id", "run_timestamp"] + _RESULT_COLUMNS)

    try:
        # Open the database and ensure the scan_results table exists.
        conn = _get_connection(db_path)

        # Retrieve the most recent scan records, up to the specified limit.
        df = pd.read_sql_query(
            f"SELECT * FROM scan_results ORDER BY run_timestamp DESC LIMIT {limit}",
            conn,
        )

        # Close the database connection after retrieving the records.
        conn.close()

        # Return the scan history as a pandas DataFrame.
        return df

    except Exception as exc:  # noqa: BLE001
        # Log retrieval errors and return an empty DataFrame.
        logger.error("Failed to load history: %s", exc)
        return pd.DataFrame()


def clear_history(db_path: Path = _DEFAULT_DB_PATH) -> None:
    """
    Clear all records from the scan_results table in SQLite.
    """
    # Nothing needs to be cleared if the database file does not exist.
    if not db_path.exists():
        return

    try:
        # Open the database connection and ensure the table exists.
        conn = _get_connection(db_path)

        # Delete all stored scan history records.
        conn.execute("DELETE FROM scan_results")
        conn.commit()

        # Reclaim unused database space after deleting the records.
        conn.execute("VACUUM")

        # Close the connection after clearing the history.
        conn.close()

        # Record a successful history-clearing operation.
        logger.info("Cleared scan history.")

    except Exception as exc:  # noqa: BLE001
        # Log any error that occurs while clearing the history.
        logger.error("Failed to clear history: %s", exc)
