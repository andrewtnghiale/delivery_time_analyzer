"""
Delivery Duration Cleaning Module
---------------------------------
Calculates and validates shipment delivery times.

This module computes the number of days each shipment took to deliver,
identifies outlier durations, and removes unrealistic values.
"""


def calculate_delivery_days(shipments_df):
    """
    Calculates delivery durations in days.

    Adds a 'delivery_days' column as the difference between
    'delivery_date' and 'ship_date'.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including date columns.

    Returns
    -------
    pandas.DataFrame
        Dataset with a new 'delivery_days' column.
    """
    # Calculate delivery duration in days
    shipments_df["delivery_days"] = (
        shipments_df["delivery_date"] - shipments_df["ship_date"]
    ).dt.days

    # Round and convert to nullable integer type
    shipments_df["delivery_days"] = (
        shipments_df["delivery_days"].round().astype("Int64")
    )

    return shipments_df


def detect_outliers(shipments_df):
    """
    Logs shipments with unrealistic delivery durations.

    Identifies shipments where 'delivery_days' fall outside 1-10 days.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including a 'delivery_days' column.

    Returns
    -------
    pandas.DataFrame
        Outlier shipments.
    """
    # Path for logging
    log_path = "data/logs/shipments_outliers.csv"

    # Identify shipments with delivery durations outside acceptable window
    outliers = shipments_df[
        (shipments_df["delivery_days"] < 1) | (shipments_df["delivery_days"] > 10)
    ]

    # Log outliers if any
    if not outliers.empty:
        outliers.to_csv(log_path, index=False)
        print(f"Saved {len(outliers)} outlier shipments to {log_path}")
    else:
        print("No unrealistic delivery durations.")

    return outliers


def remove_outliers(shipments_df):
    """
    Removes unrealistic delivery durations.

    Keeps shipments with delivery durations within 1-10 days or still in transit.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including a 'delivery_days' column.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with valid delivery durations.
    """
    # Keep shipments still in transit (no delivery date yet)
    keep_in_transit = shipments_df["delivery_days"].isna()

    # Keep only shipments with realistic delivery durations (1-10 days)
    keep_delivered_valid = shipments_df["delivery_days"].between(
        1, 10, inclusive="both"
    )

    # Combine both sets of valid records
    shipments_df = shipments_df[keep_in_transit | keep_delivered_valid]

    return shipments_df


def handle_delivery_durations(shipments_df):
    """
    Executes delivery duration and outlier handling:
      1. Calculate delivery durations.
      2. Log unrealistic delivery times.
      3. Remove outlier shipments.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'ship_date' and 'delivery_date' columns.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with valid delivery durations.
    """
    shipments_df = calculate_delivery_days(shipments_df)
    detect_outliers(shipments_df)
    shipments_df = remove_outliers(shipments_df)

    return shipments_df
