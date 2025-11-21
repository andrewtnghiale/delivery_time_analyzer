"""
Export all SQL analysis query results to CSV files for Tableau.

This script loads the SQLite database, imports all the query
functions from the analysis modules, runs every query, and saves
each result as a CSV file inside:

    data/tableau_exports/

Run using:
    python -m src.sql_analysis.export_queries
"""

import os
import sqlite3
import pandas as pd

# ===== Import query modules =====
from src.sql_analysis.connections_and_overview import (
    count_total_shipments,
    count_by_status,
    count_delivery_days,
    avg_delivery_days,
    avg_delay_late_shipments,
    delay_count
)

from src.sql_analysis.holiday_analysis import (
    avg_delivery_days_by_holiday,
    shipment_volume_by_holiday,
    holiday_region_performance,
    holiday_delay_distribution,
    holiday_delay_gap,
    holiday_success_rate,
    holiday_in_transit_ratio,
    holiday_volume_spike,
    holiday_weekly_trends,
    holiday_vs_region_success
)

from src.sql_analysis.region_analysis import (
    count_origin_regions,
    count_destination_regions,
    avg_delivery_days_by_region,
    volume_by_region,
    in_transit_by_region,
    avg_delivery_days_by_route,
    on_time_rate_by_region,
    delivery_success_rate,
    delivery_success_rate_per_region,
    slowest_routes,
    fastest_routes,
    bottleneck_routes,
    region_performance_summary
)

from src.sql_analysis.trend_analysis import (
    weekly_volume,
    volume_by_day_of_week,
    avg_delivery_by_day_of_week,
    weekly_avg_delivery_days,
    weekly_in_transit
)



# ============================================================
# Helper: Save any DataFrame to CSV
# ============================================================

def save(df, name, export_dir):
    """
    Save a DataFrame to CSV with standard formatting for Tableau.
    """
    path = os.path.join(export_dir, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"[OK] Exported: {path}")


# ============================================================
# Main Export Routine
# ============================================================

def main():
    print("\n=== EXPORTING SQL ANALYSIS RESULTS FOR TABLEAU ===\n")

    # Ensure export directory exists
    export_dir = "data/tableau_exports"
    os.makedirs(export_dir, exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect("data/sqlite/shipments.db")

    # A list of (name, function) pairs
    exports = [

        # ---- Connection & Overview ----
        ("count_total_shipments", lambda conn: 
        pd.DataFrame([{"total_shipments": count_total_shipments(conn)}])),
        ("count_by_status", count_by_status),
        ("count_delivery_days", count_delivery_days),
        ("avg_delivery_days", lambda conn: pd.DataFrame([{"avg_delivery_days": avg_delivery_days(conn)}])),
        ("avg_delay_late_shipments", avg_delay_late_shipments),
        ("delay_count", lambda conn: pd.DataFrame([{"delay_count": delay_count(conn)}])),

        # ---- Holiday Queries ----
        ("avg_delivery_days_by_holiday", lambda conn: pd.DataFrame([{"avg_delivery_days_by_holiday": avg_delivery_days_by_holiday(conn)}])),
        ("shipment_volume_by_holiday", shipment_volume_by_holiday),
        ("holiday_region_performance", holiday_region_performance),
        ("holiday_delay_distribution", holiday_delay_distribution),
        ("holiday_delay_gap", holiday_delay_gap),
        ("holiday_success_rate", holiday_success_rate),
        ("holiday_in_transit_ratio", holiday_in_transit_ratio),
        ("holiday_volume_spike", holiday_volume_spike),
        ("holiday_weekly_trends", holiday_weekly_trends),
        ("holiday_vs_region_success", holiday_vs_region_success),

        # ---- Region Queries ----
        ("count_origin_regions", count_origin_regions),
        ("count_destination_regions", count_destination_regions),
        ("avg_delivery_days_by_region", avg_delivery_days_by_region),
        ("volume_by_region", volume_by_region),
        ("in_transit_by_region", in_transit_by_region),
        ("avg_delivery_days_by_route", avg_delivery_days_by_route),
        ("on_time_rate_by_region", on_time_rate_by_region),
        ("delivery_success_rate", lambda conn: pd.DataFrame([{"delivery_success_rate": delivery_success_rate(conn)}])),
        ("delivery_success_rate_per_region", delivery_success_rate_per_region),
        ("slowest_routes", slowest_routes),
        ("fastest_routes", fastest_routes),
        ("bottleneck_routes", bottleneck_routes),
        ("region_performance_summary", region_performance_summary),

        # ---- Trend Queries ----
        ("weekly_volume", weekly_volume),
        ("volume_by_day_of_week", volume_by_day_of_week),
        ("avg_delivery_by_day_of_week", avg_delivery_by_day_of_week),
        ("weekly_avg_delivery_days", weekly_avg_delivery_days),
        ("weekly_in_transit", weekly_in_transit),
    ]

    # Run and export
    for name, func in exports:
        print(f"Running: {name} ...")
        df = func(conn)
        save(df, name, export_dir)

    conn.close()
    print("\n=== ALL EXPORTS COMPLETE! ===\n")


if __name__ == "__main__":
    main()
