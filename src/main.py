"""
Shipment Data ETL Pipeline
--------------------------
Main driver script for the shipment data-cleaning workflow.

Performs the full ETL process:
1. Extract - Load raw shipment data from CSV.
2. Transform - Clean, validate, and standardize records using
   functions from 'src.cleaning'.
3. Load - Export the cleaned dataset for SQL analysis or reporting.
"""

# ============================================================
# IMPORTS
# ============================================================

from cleaning.load_data import load_data

from cleaning.ids_cleaning import (
    fix_missing_ids,
    remove_duplicate_shipment_ids
)

from cleaning.date_cleaning import (
    handle_missing_ship_dates,
    handle_missing_delivery_date,
    handle_invalid_delivery_date,
    handle_both_missing_dates
)

from cleaning.region_cleaning import (
    normalize_region_format,
    log_missing_regions,
    validate_and_correct_regions,
    clean_regions,
    drop_shipments_missing_both_regions,
    drop_shipments_with_missing_region,
    remove_invalid_region_strings
)

from cleaning.duration_cleaning import (
    calculate_delivery_days,
    detect_outliers,
    remove_outliers,
    handle_delivery_durations
)

from cleaning.save_data import save_cleaned_data


def main():
    """
    Executes the complete shipment ETL pipeline.

    Runs all data-cleaning operations, including:
      - Extracting raw shipment data from CSV.
      - Cleaning and validating shipment IDs, dates, and regions.
      - Calculating delivery durations and removing outliers.
      - Exporting the final standardized dataset.

    Prints summary outputs and saves the cleaned data to disk.
    """
    print("Starting ETL Pipeline...")

    # ============================================================
    # EXTRACT
    # ============================================================
    shipments_df = load_data("data/raw/shipments.csv")

    # ============================================================
    # TRANSFORM - ID CLEANING
    # ============================================================
    shipments_df = fix_missing_ids(shipments_df)
    shipments_df = remove_duplicate_shipment_ids(shipments_df)

    # ============================================================
    # TRANSFORM - DATE CLEANING
    # ============================================================
    shipments_df = handle_missing_ship_dates(shipments_df)
    shipments_df = handle_invalid_delivery_date(shipments_df)
    shipments_df = handle_missing_delivery_date(shipments_df)
    shipments_df = handle_both_missing_dates(shipments_df)

    # ============================================================
    # TRANSFORM - REGION CLEANING
    # ============================================================
    shipments_df = normalize_region_format(shipments_df)
    shipments_df = log_missing_regions(shipments_df)
    shipments_df = validate_and_correct_regions(shipments_df)
    shipments_df = clean_regions(shipments_df)
    shipments_df = drop_shipments_missing_both_regions(shipments_df)
    shipments_df = drop_shipments_with_missing_region(shipments_df)
    shipments_df = remove_invalid_region_strings(shipments_df)

    # ============================================================
    # TRANSFORM - DELIVERY DURATION & OUTLIER HANDLING
    # ============================================================
    shipments_df = calculate_delivery_days(shipments_df)
    detect_outliers(shipments_df)  # logs outliers but doesn’t modify dataset
    shipments_df = remove_outliers(shipments_df)
    shipments_df = handle_delivery_durations(shipments_df)

    # ============================================================
    # LOAD
    # ============================================================
    shipments_df["shipment_id"] = shipments_df["shipment_id"].astype(int)
    save_cleaned_data(shipments_df, path="data/cleaned/shipments_cleaned.csv")

    # ============================================================
    # SUMMARY OUTPUT
    # ============================================================
    print("\nPipeline completed successfully.\n")

    print("Shipment Status Counts:")
    print(shipments_df["status"].value_counts(dropna=False))

    print("\nPreview of Cleaned Data:")
    print(shipments_df.head())

    print("\nDataFrame Info:")
    shipments_df.info()

    print("\nDescriptive Statistics:")
    print(shipments_df.describe())

if __name__ == "__main__":
    main()

    