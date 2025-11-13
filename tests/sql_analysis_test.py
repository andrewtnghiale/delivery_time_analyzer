"""
SQL Analysis Runner
-------------------
Executes predefined analytical SQL queries on the cleaned shipment dataset.

Connects to the local SQLite database created by the ETL pipeline, runs
queries from the 'sql_analysis' module, prints their results, and then
closes the database connection.
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.sql_analysis import (
    connections_and_overview,
    holiday_analysis,
    region_analysis,
    trend_analysis,
)


def main():
    """
    Runs all SQL analysis queries sequentially and prints their outputs.
    """
    conn = connections_and_overview.connect_db()

    # === Basic shipment counts and performance metrics ===
    print("BASIC SHIPMENT COUNTS & PERFORMANCE")
    print("Total shipments:", connections_and_overview.count_total_shipments(conn))
    print("Delayed shipments:", connections_and_overview.delay_count(conn))
    print("Shipments by status:\n", connections_and_overview.count_by_status(conn))
    print("Average delivery days overall:", connections_and_overview.avg_delivery_days(conn))
    print("Delivery day distribution:\n", connections_and_overview.count_delivery_days(conn))

    # === Regional performance metrics ===
    print("REGION ANALYSIS")
    print("Origin counts:\n", region_analysis.count_origin_regions(conn))
    print("Destination counts:\n", region_analysis.count_destination_regions(conn))
    print("Average delivery days by origin:\n", region_analysis.avg_delivery_days_by_region(conn))
    print("Shipment volume by origin:\n", region_analysis.volume_by_region(conn))
    print("In-transit shipments by origin:\n", region_analysis.in_transit_by_region(conn))
    print("Delivery success rate overall:", region_analysis.delivery_success_rate(conn))
    print("Delivery success rate per destination:\n",
          region_analysis.delivery_success_rate_per_region(conn))
    print("Route performance summary:\n", region_analysis.avg_delivery_days_by_route(conn))
    print("Regional performance summary:\n", region_analysis.region_performance_summary(conn))

    # === Weekly and trend performance ===
    print("TREND ANALYSIS")
    print("Weekly volume by region:\n", trend_analysis.weekly_volume(conn))
    print("Weekly average delivery days:\n", trend_analysis.weekly_avg_delivery_days(conn))
    print("Weekly in-transit shipments:\n", trend_analysis.weekly_in_transit(conn))

    # === Holiday-based analytics ===
    print("HOLIDAY ANALYSIS")
    print("Average delivery days by holiday flag:\n",
          holiday_analysis.avg_delivery_days_by_holiday(conn))
    print("Shipment volume by holiday flag:\n",
          holiday_analysis.shipment_volume_by_holiday(conn))
    print("Regional performance across holiday periods:\n",
          holiday_analysis.holiday_region_performance(conn))
    print("Delivery-day distribution during holidays:\n",
          holiday_analysis.holiday_delay_distribution(conn))
    print("Holiday delivery success rates:\n",
          holiday_analysis.holiday_success_rate(conn))
    print("Holiday in-transit ratios:\n",
          holiday_analysis.holiday_in_transit_ratio(conn))
    print("Holiday weekly trends:\n",
          holiday_analysis.holiday_weekly_trends(conn))
    print(holiday_analysis.holiday_vs_region_success(conn))

    conn.close()
    print("ANALYSIS COMPLETE")


if __name__ == "__main__":
    main()
