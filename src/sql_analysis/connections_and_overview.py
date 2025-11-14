"""
Connection and Overview Queries
-------------------------------
Functions for connecting to the SQLite database and retrieving
high-level shipment statistics from the 'shipments_cleaned' table.

Includes:
    - Database connection setup.
    - Total shipment count.
    - Shipment count by status (Delivered vs. In Transit).
    - Delivery-day frequency distribution.
    - Average delivery duration across all records.
    - Count of delayed or in-transit shipments.
"""

import sqlite3
import pandas as pd


def connect_db():
    """Connect to the SQLite database and return a connection object."""
    conn = sqlite3.connect("data/sqlite/shipments.db")
    return conn


def count_total_shipments(conn):
    """Return the total number of shipment records in the database."""
    query = """
    SELECT 
        COUNT(shipment_id) AS total_shipments
    FROM shipments_cleaned;
    """
    df = pd.read_sql_query(query, conn)
    return df.iloc[0]["total_shipments"]


def count_by_status(conn):
    """Return shipment counts grouped by 'Delivered and 'In Transit'."""
    query = """
    SELECT
        status,
        COUNT(*) AS shipment_count
    FROM shipments_cleaned
    WHERE status IN  ('Delivered', 'In Transit')
    GROUP BY status
    ORDER BY shipment_count DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def count_delivery_days(conn):
    """Return number of deliveries grouped by delivery days (1-5)."""
    query = """
    SELECT
        delivery_days,
        COUNT(*) AS delivery_days_count
    FROM shipments_cleaned
    WHERE delivery_days IN (1, 2, 3, 4, 5)
    GROUP BY delivery_days
    ORDER BY delivery_days;
    """
    df = pd.read_sql_query(query, conn)
    return df


def avg_delivery_days(conn):
    """
    Return the average number of delivery days for all shipments.

    Includes both delivered and in-transit shipments unless filtered elsewhere.
    """
    query = """
    SELECT
        ROUND(AVG(delivery_days), 2) AS delivery_days_avg
    FROM shipments_cleaned
    """
    df = pd.read_sql_query(query, conn)
    return round(df.iloc[0]["delivery_days_avg"])


def avg_delay_late_shipments(conn, sla_days=4):
    """
    Average delivery days for shipments that exceed the SLA threshold.
    """
    query = f"""
    SELECT ROUND(AVG(delivery_days), 2) AS avg_late_delivery_days
    FROM shipments_cleaned
    WHERE delivery_days > {sla_days};
    """
    return pd.read_sql_query(query, conn)


def delay_count(conn):
    """Return the number of shipments currently marked as 'In Transit'."""
    query = """
    SELECT
        COUNT(*) AS delay_count
    FROM shipments_cleaned
    WHERE status = "In Transit";
    """
    df = pd.read_sql_query(query, conn)
    return df.iloc[0]["delay_count"]
