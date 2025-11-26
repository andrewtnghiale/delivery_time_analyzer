"""
Pipeline runner for Delivery Time Analyzer.

Runs:
1. Mock data generation
2. ETL cleaning pipeline
3. Load cleaned data into SQLite
4. SQL analysis scripts
"""

import subprocess
import sys
from pathlib import Path


def run_step(description: str, script_path: Path) -> None:
    print(f"\n{description}...")
    subprocess.run([sys.executable, str(script_path)], check=True)
    print(f"{description} complete.")


def main():
    base_dir = Path(__file__).resolve().parent
    run_step(
        "Generating mock shipment data",
        base_dir / "data_generation" / "generate_mock_data.py",
    )

    run_step(
        "Running ETL pipeline",
        base_dir / "src" / "main.py",
    )

    run_step(
        "Loading cleaned data into SQLite",
        base_dir / "src" / "database" / "load_to_sqlite.py",
    )

    sql_analysis_dir = base_dir / "src" / "sql_analysis"

    run_step(
        "Running connections and overview analysis",
        sql_analysis_dir / "connections_and_overview.py",
    )
    run_step(
        "Running holiday analysis",
        sql_analysis_dir / "holiday_analysis.py",
    )
    run_step(
        "Running region analysis",
        sql_analysis_dir / "region_analysis.py",
    )
    run_step(
        "Running trend analysis",
        sql_analysis_dir / "trend_analysis.py",
    )

    print("\nAll pipeline steps completed successfully!")


if __name__ == "__main__":
    main()
