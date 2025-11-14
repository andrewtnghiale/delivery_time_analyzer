"""
Trend Analysis
--------------
SQL queries for analyzing weekly and time-based shipment trends
using the 'shipments_cleaned' table.

Includes:
    - Weekly shipment volumes by destination region.
    - Weekly average delivery durations.
    - Weekly in-transit shipment counts.
    - Time-based delivery performance patterns.
"""

import pandas as pd


def weekly_volume(conn):
    """
    Return weekly shipment volume statistics per destination region.

    Columns:
        - delivered_week_number: Week number (0-53)
        - delivery_count: Count of 'Delivered' shipments
        - in_transit_count: Count of 'In Transit' shipments
    """
    query = """
    SELECT
        destination_region,
        strftime('%W', delivery_date) AS delivered_week_number,
        COUNT(CASE WHEN status = 'Delivered' THEN 1 END) AS delivery_count,
        COUNT(CASE WHEN status = 'In Transit' THEN 1 END) AS in_transit_count
    FROM shipments_cleaned
    GROUP BY destination_region, delivered_week_number
    ORDER BY delivered_week_number ASC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def volume_by_day_of_week(conn):
    """
    Shipment volume grouped by day of week.
    Monday = 0, Sunday = 6.
    """
    query = """
    SELECT 
        STRFTIME('%w', ship_date) AS day_of_week,
        COUNT(*) AS shipment_volume
    FROM shipments_cleaned
    GROUP BY day_of_week
    ORDER BY day_of_week;
    """
    return pd.read_sql_query(query, conn)


def avg_delivery_by_day_of_week(conn):
    """
    Average delivery days grouped by day of week.
    Monday = 0, Sunday = 6.
    """
    query = """
    SELECT 
        STRFTIME('%w', ship_date) AS day_of_week,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY day_of_week
    ORDER BY day_of_week;
    """
    return pd.read_sql_query(query, conn)


def weekly_avg_delivery_days(conn):
    """Return the average delivery days per destination region for each week."""
    query = """
    SELECT 
        destination_region,
        strftime('%W', delivery_date) AS delivered_week_number,
        ROUND(AVG(delivery_days), 2) AS avg_deliveries
    FROM shipments_cleaned
    GROUP BY destination_region, delivered_week_number
    ORDER BY delivered_week_number ASC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def weekly_in_transit(conn):
    """Return number of shipments in transit per shipping week."""
    query = """
    SELECT 
        strftime('%W', ship_date) AS weeks,
        COUNT(CASE WHEN status = 'In Transit' THEN 1 END) AS shipments_in_transit
    FROM shipments_cleaned
    GROUP BY weeks
    ORDER BY weeks ASC;
    """
    df = pd.read_sql_query(query, conn)
    return df
