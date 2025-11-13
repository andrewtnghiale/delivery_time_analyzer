import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os

conn = sqlite3.connect("data/sqlite/shipments.db")

# Load data
df = pd.read_sql_query("SELECT * FROM shipments_cleaned", conn)

# Ensure output folder exists
os.makedirs("data/visuals", exist_ok=True)

# Example: Shipment Volume by Status
status_counts = df["status"].value_counts()
status_counts.plot(kind="bar", title="Shipment Volume by Status", color="skyblue")
plt.xlabel("Status")
plt.ylabel("Number of Shipments")
plt.tight_layout()
plt.savefig("data/visuals/shipment_volume_by_status.png", dpi=300)
plt.xticks(rotation=0)
plt.show()
