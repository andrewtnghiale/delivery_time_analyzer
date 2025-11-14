"""
Region-Based Shipment Analysis
------------------------------
SQL queries analyzing shipment performance by origin and destination
region using the 'shipments_cleaned' table.

Includes:
    - Shipment counts by origin and destination region.
    - Total shipment volumes and in-transit counts by region.
    - Average delivery days per region and per route.
    - Overall and per-region delivery success rates.
    - Region-level performance summaries.
"""

import pandas as pd


def count_origin_regions(conn):
    """Return shipment counts for each region."""
    query = """
    SELECT
        origin_region,
        COUNT(*) AS origin_count
    FROM shipments_cleaned
    WHERE origin_region IN ('North', 'South', 'East', 'West')
    GROUP BY origin_region
    ORDER BY origin_count DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def count_destination_regions(conn):
    """Return shipment counts for each destination region."""
    query = """
    SELECT
        destination_region,
        COUNT(*) AS destination_count
    FROM shipments_cleaned
    WHERE destination_region IN ('North', 'South', 'East', 'West')
    GROUP BY destination_region
    ORDER BY destination_count DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def avg_delivery_days_by_region(conn):
    """Return the average delivery days for each origin region."""
    query = """
    SELECT
        origin_region,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    WHERE status = 'Delivered'
    GROUP BY origin_region
    ORDER BY avg_delivery_days ASC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def volume_by_region(conn):
    """Return the total number of shipments sent out from each origin region."""
    query = """
    SELECT
        origin_region, COUNT(*) AS total_shipments
    FROM shipments_cleaned
    GROUP BY origin_region
    ORDER BY total_shipments DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def in_transit_by_region(conn):
    """Return the number of 'In Transit' shipments per origin region."""
    query = """
    SELECT
        origin_region, status, COUNT(*) AS total_transit
    FROM shipments_cleaned
    WHERE status = 'In Transit'
    GROUP BY origin_region
    ORDER BY total_transit DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def avg_delivery_days_by_route(conn):
    """
    Return delivery statistics per route (from origin to destination).

    Includes:
        - Total shipment volume per route
        - Average delivery days
        - Minimum and maximum delivery times

    Only includes shipments marked as 'Delivered'.
    """
    query = """
    SELECT
        origin_region,
        destination_region,
        COUNT(*) AS shipment_volume,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
        MIN(delivery_days),
        MAX(delivery_days)
    FROM shipments_cleaned
    WHERE status = 'Delivered'
    GROUP BY origin_region, destination_region
    ORDER BY avg_delivery_days ASC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def on_time_rate_by_region(conn, sla_days=4):
    """
    On-time delivery percentage by destination region.
    A shipment is on-time if delivery_days <= SLA.
    """
    query = f"""
    SELECT 
        destination_region,
        ROUND(
            SUM(CASE WHEN delivery_days <= {sla_days} THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS on_time_rate
    FROM shipments_cleaned
    GROUP BY destination_region
    ORDER BY on_time_rate DESC;
    """
    return pd.read_sql_query(query, conn)


def delivery_success_rate(conn):
    """Return overall percentage of successfully delivered shipments."""
    query = """
    SELECT
        ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100.0 / COUNT(*), 2)
        AS delivery_success_rate
    FROM shipments_cleaned;
    """
    df = pd.read_sql_query(query, conn)
    return df.iloc[0]["delivery_success_rate"]


def delivery_success_rate_per_region(conn):
    """Return delivery success percentage for each destination region."""
    query = """
    SELECT
        destination_region,
        ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100.0 / COUNT(*), 2)
        AS delivery_success_rate
    FROM shipments_cleaned
    GROUP BY destination_region
    ORDER BY delivery_success_rate DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def slowest_routes(conn, limit=5):
    """
    Top N slowest origin→destination routes by average delivery days.
    """
    query = f"""
    SELECT 
        origin_region,
        destination_region,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY origin_region, destination_region
    ORDER BY avg_delivery_days DESC
    LIMIT {limit};
    """
    return pd.read_sql_query(query, conn)


def fastest_routes(conn, limit=5):
    """
    Top N fastest origin→destination routes by average delivery days.
    """
    query = f"""
    SELECT 
        origin_region,
        destination_region,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY origin_region, destination_region
    ORDER BY avg_delivery_days ASC
    LIMIT {limit};
    """
    return pd.read_sql_query(query, conn)


def bottleneck_routes(conn, limit=10):
    """
    Identifies bottleneck routes with both high volume and slow delivery speeds.
    """
    query = f"""
    SELECT 
        origin_region,
        destination_region,
        COUNT(*) AS shipment_volume,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY origin_region, destination_region
    HAVING COUNT(*) > 0
    ORDER BY shipment_volume DESC, avg_delivery_days DESC
    LIMIT {limit};
    """
    return pd.read_sql_query(query, conn)


def region_performance_summary(conn):
    """
    Return a summarized performance report for each destination region.

    Includes:
        - Total shipments
        - Delivered and in-transit counts
        - Delivered success rate (percentage)
        - Average, minimum, and maximum delivery days
    """
    query = """
    SELECT
        destination_region,
        COUNT(shipment_id) AS total_shipments,
        COUNT(CASE WHEN status = 'Delivered' THEN 1 END) AS delivered_shipments,
        COUNT(CASE WHEN status = 'In Transit' THEN 1 END) AS in_transit_shipments,
        ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100.0 / COUNT(*), 2)
        AS delivery_success_rate,
        ROUND(AVG(delivery_days), 2),
        MIN(delivery_days) AS min_delivery_days,
        MAX(delivery_days) AS max_delivery_days
    FROM shipments_cleaned
    GROUP BY destination_region
    ORDER BY delivery_success_rate DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df
