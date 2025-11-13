"""
Generate a mock shipment dataset for ETL and analysis.

Simulates realistic shipment activity with seasonal surges and data quality issues.
Each record includes:
  - A unique shipment ID.
  - A randomized ship date within the past 90 days.
  - A delivery date based on shipping delay (1-5 days normally, 2-8 days during holidays).
  - Randomly assigned origin and destination regions (North, South, East, West).
  - A holiday flag indicating whether the shipment occurred during a peak period.

The dataset also injects a small percentage of intentional errors to simulate real-world
data cleaning scenarios, such as:
  - Missing or duplicate shipment IDs.
  - Missing or invalid ship/delivery dates.
  - Delivery dates earlier than ship dates.
  - Inconsistent or malformed region text.
  - Missing origin/destination regions.
  - Extreme delivery durations (negative or unusually long).

The generated dataset is saved as a CSV file for later cleaning and analysis.

Output
------
data/raw/shipments.csv
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# Number of shipment records to generate
# Possible region names for origin / destination
# List to store generated shipment
num_of_records = 2550
regions = ["North", "South", "East", "West"]
data = []

# Holiday windows for seasonal surges and delays
holiday_windows = [
    ("2025-11-20", "2025-11-30"),  # Thanksgiving rush
    ("2025-12-15", "2025-12-31"),  # Christmas / New Year rush
]

for shipment_id in range(1, num_of_records + 1):
    # Assign ship date
    # 35% of shipments occur during holiday windows
    # The rest occur randomly within the past 90 days
    if random.random() < 0.35:
        start, end = random.choice(holiday_windows)
        start_dt, end_dt = datetime.strptime(start, "%Y-%m-%d"), datetime.strptime(
            end, "%Y-%m-%d"
        )
        ship_date = start_dt + timedelta(
            days=random.randint(0, (end_dt - start_dt).days)
        )
    else:
        ship_date = datetime.today() - timedelta(days=random.randint(1, 90))

    # Check if shipment date falls in holiday window
    is_holiday = any(
        datetime.strptime(start, "%Y-%m-%d")
        <= ship_date
        <= datetime.strptime(end, "%Y-%m-%d")
        for start, end in holiday_windows
    )

    # Assign delivery delay based on season
    if is_holiday:
        if random.random() < 0.15:
            # In Transit: No delivery
            delivery_date = None
        elif random.random() < 0.25:
            # Severe delay
            delay_days = random.randint(9, 14)
            delivery_date = ship_date + timedelta(days=delay_days)
        else:
            # Normal holiday delay
            delay_days = random.randint(2, 8)
            delivery_date = ship_date + timedelta(days=delay_days)
    else:
        # Regular season
        if random.random() < 0.05:
            delivery_date = None
        else:
            delay_days = random.randint(1, 5)
            delivery_date = ship_date + timedelta(days=delay_days)

    # Assign random origin/destination regions
    origin = random.choice(regions)
    destination = random.choice(regions)

    # Introduce a small percentage of records with invalid or missing data
    bad_record_chance = random.random()

    if bad_record_chance < 0.03:
        # Missing shipment ID
        shipment_id = None
    elif bad_record_chance < 0.07:
        # Missing delivery date
        delivery_date = None
    elif bad_record_chance < 0.09:
        # Missing both dates
        ship_date = None
        delivery_date = None
    elif bad_record_chance < 0.11:
        # Delivery before shipment
        delivery_date = ship_date - timedelta(days=random.randint(1, 3))
    elif bad_record_chance < 0.16:
        # Random inconsistent region formatting
        origin = random.choice([" north ", "south", "EAST", "west"])
        destination = random.choice(["north", " South", "east", "WEST "])
    elif bad_record_chance < 0.20:
        # Duplicate shipment ID
        shipment_id = random.randint(1, num_of_records // 10)
    elif bad_record_chance < 0.23:
        # Randomly missing origin/destination region
        origin = "" if random.random() < 0.5 else random.choice(regions)
        destination = "" if random.random() < 0.5 else random.choice(regions)
    elif bad_record_chance < 0.25:
        # Extreme delivery duration
        delivery_date = ship_date + timedelta(days=random.randint(-10, 30))
    else:
        # Normal record with valid data
        origin = random.choice(regions)
        destination = random.choice(regions)

    # Creates the shipment record dictionary
    record = {
        "shipment_id": shipment_id,
        "ship_date": ship_date.strftime("%Y-%m-%d") if ship_date else "",
        "delivery_date": delivery_date.strftime("%Y-%m-%d") if delivery_date else "",
        "origin_region": origin,
        "destination_region": destination,
        "holiday_period": "Yes" if is_holiday else "No",
    }
    data.append(record)

# Convert all shipment records into pandas DataFrame
# Save Dataframe as a CSV file
shipments_df = pd.DataFrame(data)
shipments_df.to_csv("data/raw/shipments.csv", index=False)
print(f"Generated {num_of_records} shipments dataset in data/raw/shipments.csv")
