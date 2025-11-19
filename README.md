# Delivery Time Analyzer

## A Full ETL and Analytics Pipeline for Shipment Performance Insights

This project simulates and processes logistics shipment data to replicate real-world data analytics challenges. It provides a complete ETL (Extract, Transform, Load) pipeline in Python, cleaning and validating data before loading it into an SQLite database and analyzing it using SQL and business intelligence tools like Tableau.
This pipeline is designed with data realism in mind: the raw data contains intentionally injected errors such as missing IDs, malformed regions, invalid dates, and outliers. Each cleaning stage generates detailed logs to mimic enterprise data governance practices.

## Features
### ETL Pipeline
**Extract**:
- Load raw shipment CSV files

**Transform**:
- **Shipment ID Cleaning**  
  Fix missing IDs, remove duplicates, log all changes

- **Date Cleaning**  
  Remove invalid or missing dates, standardize status, ensure chronological logic

- **Delivery Duration Logic**  
  Calculate `delivery_days`, detect and remove outlier durations

- **Region Standardization**  
  Normalize region names, correct misspellings, log or drop records with missing or invalid regions

- **Audit Logging**  
  Create detailed log files for all cleaning actions

**Load**:
- Save cleaned dataset to CSV
- Load processed data into SQLite for analytics and dashboards

### Shipment Data Analytics
**SQL modules produce insights including**:
- **Overview and Status Metrics**  
  Total shipments, delivered vs. in-transit counts, average delivery time, and delivery frequency distribution.

- **Holiday Performance Analysis**  
  Compare shipment performance during holiday vs. non-holiday periods; analyze regional holiday impact and delay gaps.

- **Region-Based Insights**  
  Shipment volumes by origin/destination, average delivery days per region and route, delivery success rates, bottleneck and fastest routes.

- **Trend and Time-Based Analysis**  
  Weekly shipment volume trends, day-of-week performance, transit volume over time, and weekly delivery duration averages.

Modular and Scalable
- Each cleaning and analysis process is developed as a separate module under src/, making this project modular, testable, and extensible.


## Project Structure

delivery_time_analyzer/

├── data/

│   ├── raw/                # Unprocessed shipment CSVs

│   ├── cleaned/            # Final cleaned CSV outputs

│   ├── logs/               # Audit logs for data issues

│   └── sqlite/

│       └── shipments.db    # SQLite database for analytics

│

├── visuals/                # Generated charts / exports (PNG, PDF, etc.)

│

├── data_generation/

│   └── generate_mock_data.py   # Synthetic shipment dataset generator

│

├── src/


│   ├── main.py                 # Main ETL pipeline runner

│

│   ├── charts/

│   │   ├── visualize_trends.py

│   │   └── __init__.py

│

│   ├── cleaning/               # Data cleaning modules (Transform step)

│   │   ├── load_data.py

│   │   ├── ids_cleaning.py

│   │   ├── date_cleaning.py

│   │   ├── duration_cleaning.py

│   │   ├── region_cleaning.py

│   │   ├── save_data.py

│   │   └── __init__.py

│

│   ├── database/

│   │   └── load_to_sqlite.py

│

│   └── sql_analysis/

│       ├── connections_and_overview.py

│       ├── holiday_analysis.py

│       ├── region_analysis.py

│       ├── trend_analysis.py

│       └── __init__.py

│

├── tests/

│

├── .gitignore

├── .gitattributes

└── README.md


## How to Run

### 1. Create and activate a virtual environment

```sh
python -m venv venv
.\venv\Scripts\activate      # Windows
```
### 2. Install dependencies

```sh
pip install -r requirements.txt
```

### 3. Generate mock shipment data
This creates the raw file in: data/raw/shipments.csv
```sh
python data_generation/generate_mock_data.py
```

### 4. Run the full ETL pipeline
This step loads raw data, cleans it, logs issues, and then outputs into a cleaned CSV.
```sh
python src/main.py
```

### 5. Load cleaned data into SQLite
Creates or updates: data/sqlite/shipments.db
```sh
python src/database/load_to_sqlite.py
```

### 6. Run SQL analytic scripts
Each script performs a focused analysis.
```sh
python src/sql_analysis/connections_and_overview.py
python src/sql_analysis/holiday_analysis.py
python src/sql_analysis/region_analysis.py
python src/sql_analysis/trend_analysis.py
```

### 7. Generate visualizations (Optional)
Outputs charts to the visuals/ folder:
```sh
python src/charts/visualize_trends.py
```