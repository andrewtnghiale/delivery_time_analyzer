"""
Region Cleaning Module
----------------------
Standardizes and validates region-related fields in shipment data.

This module normalizes region name formatting, identifies missing or inconsistent
regions, and corrects common misspellings. Ensures that all region entries
conform to valid categories (North, South, East, West) while maintaining logs
for invalid or incomplete data.
"""


def normalize_region_format(shipments_df):
    """
    Standardizes region text formatting.

    Strips whitespace and converts region names to title case.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'origin_region' and 'destination_region' columns.

    Returns
    -------
    pandas.DataFrame
        Dataset with normalized region names.
    """
    # Remove leading/trailing spaces and convert to title case
    shipments_df["origin_region"] = (
        shipments_df["origin_region"].str.strip().str.title()
    )
    shipments_df["destination_region"] = (
        shipments_df["destination_region"].str.strip().str.title()
    )

    return shipments_df


def log_missing_regions(shipments_df):
    """
    Logs shipments with missing or inconsistent region data.

    Exports three categories:
      1. Missing origin region.
      2. Missing destination region.
      3. Delivered shipments missing one or both regions.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'origin_region', 'destination_region', and 'status' columns.

    Returns
    -------
    pandas.DataFrame
        Original dataset (no rows removed).
    """
    # Paths for logging
    log_path_origin = "data/logs/shipments_missing_origin_region.csv"
    log_path_destination = "data/logs/shipments_missing_destination_region.csv"
    log_path_inconsistent = "data/logs/shipments_inconsistent_regions.csv"

    # Identify shipments missing origin or destination regions
    missing_origin = shipments_df[shipments_df["origin_region"].isna()]
    missing_destination = shipments_df[shipments_df["destination_region"].isna()]

    # Log missing origin region records
    if not missing_origin.empty:
        missing_origin.to_csv(log_path_origin, index=False)
        print(
            f"Saved {len(missing_origin)} missing origin regions to {log_path_origin}"
        )
    else:
        print("No missing origin regions.")

    # Log missing destination region records
    if not missing_destination.empty:
        missing_destination.to_csv(log_path_destination, index=False)
        print(
            f"Saved {len(missing_destination)} missing destination regions "
            f"to {log_path_destination}"
        )
    else:
        print("No missing destination regions.")

    # Identify inconsistent shipments: Delivered but missing regions
    inconsistent_shipments = shipments_df[
        (shipments_df["status"] == "Delivered")
        & (
            shipments_df["origin_region"].isna()
            | shipments_df["destination_region"].isna()
        )
    ]

    # Log inconsistent shipments
    if not inconsistent_shipments.empty:
        inconsistent_shipments.to_csv(log_path_inconsistent, index=False)
        print(
            f"Saved {len(inconsistent_shipments)} inconsistent shipments "
            f"(Delivered with missing regions) to {log_path_inconsistent}"
        )

    return shipments_df


def validate_and_correct_regions(shipments_df):
    """
    Validates and corrects region names.

    Replaces common misspellings, checks validity, and logs any remaining
    invalid entries.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'origin_region' and 'destination_region' columns.

    Returns
    -------
    pandas.DataFrame
        Dataset with corrected and validated region names.
    """
    # Define valid regions and correction dictionary
    valid_regions = ["North", "South", "East", "West"]
    corrections = {"Noth": "North", "Soth": "South", "Eest": "East", "Wes": "West"}

    # Path for logging
    log_path = "data/logs/shipments_invalid_regions.csv"

    # Apply corrections for known misspellings
    shipments_df.replace(corrections, inplace=True)

    # Identify entries with invalid or unknown region names
    invalid_regions = shipments_df[
        (~shipments_df["origin_region"].isin(valid_regions))
        | (~shipments_df["destination_region"].isin(valid_regions))
    ]

    # Log remaining invalid region entries
    if not invalid_regions.empty:
        invalid_regions.to_csv(log_path, index=False)
        print(
            f"Saved {len(invalid_regions)} invalid region entries for review in {log_path}."
        )
    else:
        print("No incorrect or mispelled regions.")

    return shipments_df


def clean_regions(shipments_df):
    """
    Executes the region-cleaning workflow:
      1. Normalize region text format.
      2. Log missing or inconsistent regions.
      3. Validate and correct misspellings.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including region and status fields.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with standardized region data.
    """
    shipments_df = normalize_region_format(shipments_df)
    shipments_df = log_missing_regions(shipments_df)
    shipments_df = validate_and_correct_regions(shipments_df)

    return shipments_df


def drop_shipments_missing_both_regions(shipments_df):
    """
    Removes shipments missing both region fields.

    Logs and removes shipments missing both 'origin_region' and
    'destination_region'.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'origin_region' and 'destination_region' columns.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with both-region-missing records removed.
    """
    # Path for logging
    log_path = "data/logs/shipments_missing_both_regions.csv"

    # Identify shipments missing both origin and destination regions
    missing_both = shipments_df[
        (
            shipments_df["origin_region"].isna()
            | (shipments_df["origin_region"].astype(str).str.strip() == "")
        )
        & (
            shipments_df["destination_region"].isna()
            | (shipments_df["destination_region"].astype(str).str.strip() == "")
        )
    ]

    # If such shipments exist, log and remove them
    if not missing_both.empty:
        missing_both.to_csv(log_path, index=False)
        print(f"Saved {len(missing_both)} shipments missing both regions to {log_path}")
        shipments_df = shipments_df.drop(missing_both.index)
        print(f"Removed {len(missing_both)} shipments missing both regions.")
    else:
        print("No shipments missing both regions found.")

    return shipments_df


def drop_shipments_with_missing_region(shipments_df):
    """
    Removes shipments missing either origin or destination region.

    Logs and removes any shipments where one or both region fields
    are missing or blank. Should be called after region cleaning
    to ensure final dataset integrity.

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including 'origin_region' and 'destination_region' columns.

    Returns
    -------
    pandas.DataFrame
        Dataset with incomplete region data removed.
    """
    # Path for logging
    log_path = "data/logs/shipments_missing_region.csv"

    # Filtered DataFrame containing shipments with either region missing
    missing_either = shipments_df[
        (shipments_df["origin_region"].isna() | shipments_df["origin_region"].astype(str).str.strip().eq(""))
        | (shipments_df["destination_region"].isna() | shipments_df["destination_region"].astype(str).str.strip().eq(""))
    ]

    # Log and remove incomplete shipments
    if not missing_either.empty:
        missing_either.to_csv(log_path, index=False)
        print(f"Saved {len(missing_either)} shipments missing region data to {log_path}")
        shipments_df = shipments_df.drop(missing_either.index)
        print(f"Removed {len(missing_either)} shipments missing region data.")
    else:
        print("No shipments with missing regions found.")

    return shipments_df


def remove_invalid_region_strings(shipments_df):
    """
    Removes shipments where regions contain placeholder or invalid text values.

    Filters out rows where 'origin_region' or 'destination_region'
    are strings like 'None', 'NaN', or 'null' (case-insensitive).

    Parameters
    ----------
    shipments_df : pandas.DataFrame
        Dataset including region columns.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset with invalid string placeholders removed.
    """
    invalid_values = ["none", "nan", "null", "undefined", "missing", " "]

    # Define list of invalid / placeholder text values
    shipments_df["origin_region"] = shipments_df["origin_region"].astype(str)
    shipments_df["destination_region"] = shipments_df["destination_region"].astype(str)

    # Keep only shipments with region values not part of invalid_value list
    mask = (
        ~shipments_df["origin_region"].str.lower().isin(invalid_values)
        & ~shipments_df["destination_region"].str.lower().isin(invalid_values)
    )

    # Log and report removals
    removed = len(shipments_df) - mask.sum()
    if removed > 0:
        print(f"Removed {removed} shipments with invalid region text values.")
        log_path = "data/logs/shipments_invalid_region_text.csv"
        shipments_df[~mask].to_csv(log_path, index=False)
        print(f"Logged invalid region text values to {log_path}")

    return shipments_df[mask]
