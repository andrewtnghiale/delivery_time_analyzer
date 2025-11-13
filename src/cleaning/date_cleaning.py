"""
Date Cleaning Module
--------------------
Handles validation and correction of shipment date fields.

Functions in this module detect and remove records with missing, reversed, 
or invalid 'ship_date' and 'delivery_date' values. Standardizes shipment 
statuses based on the presence of delivery dates and logs all issues 
for auditing and quality control.
"""

import pandas as pd


def handle_missing_ship_dates(shipments_df):
    """
    Removes shipments with missing or blank ship dates.

    Logs shipments with missing 'ship_date' values and removes them
    from the dataset.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including a 'ship_date' column.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with only valid ship dates.
    """
    # Path for logging
    log_path = "data/logs/shipments_missing_ship_date.csv"

    # Identify records with missing or empty ship dates
    missing_ship_date = shipments_df[
        shipments_df["ship_date"].isna()
        | (shipments_df["ship_date"].astype(str).str.strip() == "")
    ]

    # If such records exist, log and remove them
    if not missing_ship_date.empty:
        print(f"Found {len(missing_ship_date)} shipments missing ship dates.")

        # Save missing ship date records to log file
        missing_ship_date.to_csv(log_path, index=False)
        print(f"Saved {len(missing_ship_date)} missing ship date records to {log_path}")

        # Remove those records from main dataset
        shipments_df = shipments_df.drop(missing_ship_date.index)
    else:
        print("No missing ship dates found.")

    return shipments_df


def handle_missing_delivery_date(shipments_df):
    """
    Handles missing delivery dates by assigning shipment statuses.

    Marks shipments as "Delivered" if a delivery date exists,
    otherwise "In Transit". Converts empty strings in 'delivery_date'
    to NaT.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including a 'delivery_date' column.

    Returns
    -------
    pandas.DataFrame
        Dataset with standardized 'delivery_date' and a new 'status' column.
    """
    # Assign shipment status based on presence of delivery_date
    shipments_df["status"] = shipments_df["delivery_date"].apply(
        lambda x: "Delivered" if pd.notna(x) else "In Transit"
    )

    # Replace any remaining empty strings with NaT
    shipments_df["delivery_date"] = shipments_df["delivery_date"].replace("", pd.NaT)

    return shipments_df


def handle_invalid_delivery_date(shipments_df):
    """
    Removes shipments with invalid or reversed delivery dates.

    Logs shipments where 'delivery_date' occurs before 'ship_date'
    and removes them from the dataset.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including 'ship_date' and 'delivery_date' columns.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with only valid delivery dates.
    """
    # Path for logging
    log_path = "data/logs/shipments_invalid_delivery_dates.csv"

    # Track dataset size before cleaning
    # Identify shipments with reversed delivery dates
    before = len(shipments_df)
    invalid_dates = shipments_df[
        shipments_df["delivery_date"] < shipments_df["ship_date"]
    ]

    # Log invalid shipments if found
    if not invalid_dates.empty:
        print(f"Found {len(invalid_dates)} shipments with invalid ship dates.")
        invalid_dates.to_csv(log_path, index=False)
        print(f"Saved {len(invalid_dates)} invalid shipments to {log_path}")
    else:
        print("No invalid delivery dates found.")

    # Keep only valid shipments
    shipments_df = shipments_df[
        shipments_df["delivery_date"].isna()
        | (shipments_df["delivery_date"] >= shipments_df["ship_date"])
    ]
    print(
        f"Removed {before - len(shipments_df)} invalid, missing or reversed shipments."
    )

    return shipments_df


def handle_both_missing_dates(shipments_df):
    """
    Removes shipments missing both ship and delivery dates.

    Logs and removes shipments lacking values for both date columns.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including 'ship_date' and 'delivery_date' columns.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with both-date-missing records removed.
    """
    # Path for logging
    log_path = "data/logs/shipments_missing_both_dates.csv"

    # Identify shipments missing both ship and delivery dates
    both_missing = shipments_df[
        shipments_df["ship_date"].isna() & shipments_df["delivery_date"].isna()
    ]

    # If such records exist, log and remove them
    if not both_missing.empty:
        both_missing.to_csv(log_path, index=False)
        shipments_df = shipments_df.drop(both_missing.index)
        print(f"Removed {len(both_missing)} shipments missing both dates.")
    else:
        print("No shipments missing both dates.")

    return shipments_df
