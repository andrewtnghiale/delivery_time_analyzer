"""
Save Cleaned Shipment Data Module
---------------------------------
Exports the fully cleaned shipment dataset to a CSV file.

Formats numeric fields (such as 'shipment_id' and 'delivery_days') for
consistency and provides the final output stage in the ETL pipeline.
"""

import pandas as pd


def save_cleaned_data(shipments_df, path="data/cleaned/shipments_cleaned.csv"):
    """
    Saves the cleaned dataset to CSV.

    Formats numeric fields and exports the cleaned data to the given path.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Cleaned shipment dataset.

    path : str, optional
        Destination file path. Defaults to 'data/cleaned/shipments_cleaned.csv'.

    Returns
    -------
    pandas.DataFrame
        The same dataset after saving.
    """
    # Format 'shipment_id' and 'delivery_days' without decimals
    for col in ["shipment_id", "delivery_days"]:
        if col in shipments_df.columns:
            shipments_df[col] = shipments_df[col].apply(
                lambda x: int(x) if pd.notna(x) else ""
            )
    # Save cleaned dataset to specified path
    shipments_df.to_csv(path, index=False)
    print(f"Cleaned dataset saved to {path}")

    return shipments_df
