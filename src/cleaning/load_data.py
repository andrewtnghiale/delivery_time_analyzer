"""
Load Shipment Data Module
-------------------------
Loads raw shipment records from a CSV file and prepares them
for downstream cleaning and transformation steps.
"""

import pandas as pd


def load_data(path="data/raw/shipments.csv"):
    """
    Loads shipment data from a CSV file and prepares it for analysis.

    Reads the dataset from the specified path, replaces empty date fields
    with NaN, and converts the 'ship_date' and 'delivery_date' columns
    to datetime objects.

    Parameters
    ----------
    path : str, optional
        Path to the raw shipment CSV file. Defaults to "data/raw/shipments.csv".

    Returns
    -------
    pandas.DataFrame
        DataFrame containing shipment records with properly formatted date columns.
    """
    shipments_df = pd.read_csv(path)
    print(f"Loaded {len(shipments_df)} raw shipment records.")

    # Replace blank date strings with NaN values
    shipments_df["delivery_date"] = shipments_df["delivery_date"].replace("", pd.NA)
    shipments_df["ship_date"] = shipments_df["ship_date"].replace("", pd.NA)

    # Convert date columns to datetime objects
    shipments_df["ship_date"] = pd.to_datetime(
        shipments_df["ship_date"], errors="coerce"
    )
    shipments_df["delivery_date"] = pd.to_datetime(
        shipments_df["delivery_date"], errors="coerce"
    )

    return shipments_df
