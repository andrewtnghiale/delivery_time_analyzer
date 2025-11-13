"""
SQLite Database Creation Script
-------------------------------
Creates a local SQLite database and loads cleaned shipment data.

Reads the cleaned dataset from 'data/cleaned/shipments_cleaned.csv'
and stores it in 'data/sqlite/shipments.db' as the table
'shipments_cleaned'.
"""

import sqlite3
import pandas as pd
import os


def create_database():
    """
    Creates the SQLite database and imports cleaned shipment data.

    Ensures the database directory exists, reads the cleaned CSV,
    and writes it into the 'shipments_cleaned' table, replacing any
    existing table with the same name.
    """
    os.makedirs("data/sqlite", exist_ok=True)
    sqlite_connect = sqlite3.connect("data/sqlite/shipments.db")

    clean_shipments_df = pd.read_csv("data/cleaned/shipments_cleaned.csv")

    clean_shipments_df.to_sql(
        "shipments_cleaned", sqlite_connect, if_exists="replace", index=False
    )

    print("Completed load to sqlite")
    sqlite_connect.commit()
    sqlite_connect.close()


if __name__ == "__main__":
    create_database()