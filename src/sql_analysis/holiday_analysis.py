"""
Holiday Shipment Analytics
--------------------------
SQL queries that compare shipment performance between holiday
and non-holiday periods using the 'shipments_cleaned' table.

Includes:
    - Average delivery days by holiday period.
    - Shipment volumes during holiday vs. non-holiday periods.
    - Regional performance and delay comparisons.
    - Holiday-based delivery success and in-transit ratios.
    - Weekly shipment and delay trends across holiday periods.
    - Holiday vs. non-holiday success-rate and delay differences by region.
"""

import pandas as pd


def avg_delivery_days_by_holiday(conn):
    """Return the average delivery days for each holiday period."""
    query = """
    SELECT 
        holiday_period,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY holiday_period
    ORDER BY holiday_period DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df.iloc[0]["avg_delivery_days"]


def shipment_volume_by_holiday(conn):
    """Return total shipment volume grouped by holiday period."""
    query = """
    SELECT
        holiday_period,
        COUNT(*) AS shipment_volume
    FROM shipments_cleaned
    GROUP BY holiday_period
    ORDER BY holiday_period DESC;
    
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_region_performance(conn):
    """
    Return shipments by regional performance across holiday periods.

    Includes:
        - Shipment counts and average delivery days.
        - Delivery success rate (percentage of delivered shipments).
    """
    query = """
    SELECT
        origin_region,
        holiday_period,
        COUNT(*) AS shipment_count,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
        COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100 / COUNT(*) / 2
        AS delivery_success_rate
    FROM shipments_cleaned
    GROUP BY origin_region, holiday_period
    ORDER BY origin_region, holiday_period DESC;    
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_delay_distribution(conn):
    """Return number of delivered shipments grouped by holiday period and delivery days."""
    query = """
    SELECT
        holiday_period,
        delivery_days,
        COUNT(*) AS shipment_count
    FROM shipments_cleaned
    WHERE status = 'Delivered'
    GROUP BY holiday_period, delivery_days
    ORDER BY holiday_period, delivery_days;    
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_delay_gap(conn):
    """
    Returns the difference in average delivery days between 
    holiday and non-holiday periods.
    """
    query = """
    SELECT
        ROUND(
            (SELECT AVG(delivery_days) FROM shipments_cleaned WHERE holiday_period = 'Yes')
            -
            (SELECT AVG(delivery_days) FROM shipments_cleaned WHERE holiday_period = 'No'),
            2
        ) AS delay_gap;
    """
    return pd.read_sql_query(query, conn)


def holiday_success_rate(conn):
    """Return delivery success rate percentage by holiday period."""
    query = """
    SELECT
        holiday_period,
        ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100 / COUNT(*), 2)
        AS delivery_success_rate
    FROM shipments_cleaned
    GROUP BY holiday_period
    ORDER BY holiday_period, delivery_success_rate DESC;    
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_in_transit_ratio(conn):
    """
    Return in-transit shipment statistics by holiday period.

    Includes:
        - Total shipments and count of in-transit shipments.
        - Percentage of shipments still in transit (in_transit_rate).
    """
    query = """
    SELECT
        holiday_period,
        COUNT(CASE WHEN status = 'In Transit' THEN 1 END) AS in_transit_count,
        COUNT(*) AS total_shipments,
        ROUND(COUNT(CASE WHEN status = 'In Transit' THEN 1 END) * 100 / COUNT(*), 2)
        AS in_transit_rate
    FROM shipments_cleaned
    GROUP BY holiday_period
    ORDER BY in_transit_rate DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_volume_spike(conn):
    """
    Percentage increase in shipment volume during holiday periods
    compared to non-holiday periods.
    """
    query = """
    SELECT
        ROUND(
            (
                (SELECT COUNT(*) FROM shipments_cleaned WHERE holiday_period = 'Yes') -
                (SELECT COUNT(*) FROM shipments_cleaned WHERE holiday_period = 'No')
            ) * 100.0 /
            (SELECT COUNT(*) FROM shipments_cleaned WHERE holiday_period = 'No'),
            2
        ) AS holiday_volume_spike_percent;
    """
    return pd.read_sql_query(query, conn)


def holiday_weekly_trends(conn):
    """
    Return weekly shipment and delivery performance during holiday periods.

    Groups shipments by week number and holiday flag to show:
        - Weekly shipment volumes.
        - Average delivery durations per week.
    """
    query = """
    SELECT
        strftime('%W', ship_date) AS week_number,
        holiday_period,
        COUNT(*) AS total_shipments,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM shipments_cleaned
    GROUP BY week_number, holiday_period
    ORDER BY week_number;
    """
    df = pd.read_sql_query(query, conn)
    return df


def holiday_vs_region_success(conn):
    """
    Compare delivery performance differences between holiday and non-holiday periods by region.

    Returns:
        - delay_difference: Average delivery-day difference (holiday − non-holiday).
        - success_rate_difference: Change in delivery success percentage between holiday and normal periods.
    """
    query = """
    SELECT
        origin_region,
        ROUND(
            AVG(CASE WHEN holiday_period = 'Yes' THEN delivery_days END) -
            AVG(CASE WHEN holiday_period = 'No' THEN delivery_days END),
        2) AS delay_difference,
        ROUND(
            (
                COUNT(CASE WHEN holiday_period='Yes' AND status='Delivered' THEN 1 END) * 100.0 /
                COUNT(CASE WHEN holiday_period='Yes' THEN 1 END)
            ) -
            (
                COUNT(CASE WHEN holiday_period='No' AND status='Delivered' THEN 1 END) * 100.0 /
                COUNT(CASE WHEN holiday_period='No' THEN 1 END)
            ),
        2) AS success_rate_difference
    FROM shipments_cleaned
    GROUP BY origin_region
    ORDER BY delay_difference DESC;
    """
    df = pd.read_sql_query(query, conn)
    return df
