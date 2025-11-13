"""
Shipment ID Cleaning Module
---------------------------
Detects and corrects missing or duplicate shipment identifiers.

This module ensures that each record in the dataset has a unique 'shipment_id'
by repairing missing values, reassigning sequential IDs, and removing duplicates.
All modifications are logged to CSV files for traceability.
"""


def fix_missing_ids(shipments_df):
    """
    Detects and repairs missing shipment IDs.

    Finds rows with missing shipment IDs, assigns new unique IDs sequentially
    after the current maximum, and saves reassigned records to a log file.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including a 'shipment_id' column.

    Returns
    -------
    pandas.DataFrame
        Updated dataset with missing 'shipment_id' values replaced.
    """
    # Path for logging
    log_path = "data/logs/shipments_reassigned_ids.csv"

    # Identify rows with missing shipment IDs
    missing_id_rows = shipments_df[shipments_df["shipment_id"].isna()]

    # if missing IDs exist, reassign them
    if not missing_id_rows.empty:
        print(
            f"Found {len(missing_id_rows)} shipments missing IDs. Reassigning new IDs..."
        )

        # Determine the next available ID range
        max_id = shipments_df["shipment_id"].max(skipna=True)
        new_ids = range(int(max_id) + 1, int(max_id) + 1 + len(missing_id_rows))

        # Replace NaN IDs with new unique values
        shipments_df.loc[shipments_df["shipment_id"].isna(), "shipment_id"] = list(
            new_ids
        )
        # Extract and save reassigned records for logging
        missing_id_rows = shipments_df[shipments_df["shipment_id"].isin(new_ids)]
        missing_id_rows.to_csv("data/logs/shipments_reassigned_ids.csv", index=False)
        print(f"Reassigned and saved {len(missing_id_rows)} shipments to {log_path}]")
    else:
        print("No missing shipment IDs found.")

    return shipments_df


def remove_duplicate_shipment_ids(shipments_df):
    """
    Removes duplicate shipment IDs.

    Detects duplicate 'shipment_id' records, logs them, and keeps only
    the first occurrence of each unique ID.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Shipment dataset including a 'shipment_id' column.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with duplicate IDs removed.
    """
    # Path for logging
    log_path = "data/logs/shipments_duplicate_ids.csv"

    # Identify duplicate shiment IDs
    duplicate_rows = shipments_df[shipments_df["shipment_id"].duplicated(keep=False)]

    if not duplicate_rows.empty:
        print(f"Found {len(duplicate_rows)} duplicate shipment records.")

        # Save all duplicate entries to log file
        duplicate_rows.to_csv(log_path, index=False)
        print(f"Saved duplicate shipment records to {log_path}")

        # Remove duplicates, keeping only the first occurrence
        shipments_df = shipments_df.drop_duplicates(
            subset=["shipment_id"], keep="first"
        )
        print(f"Removed {len(duplicate_rows)} duplicate shipment records from dataset.")
    else:
        print("No duplicate shipment IDs found.")

    return shipments_df
