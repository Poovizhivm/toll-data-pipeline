#!/usr/bin/env bash
# ==============================================================================
# Extract_Transform_data.sh
# Phase 1 — Shell ETL
# ==============================================================================

set -euo pipefail

# Determine data directory: allow overriding via DATA_DIR or default relative to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/../data}"

echo "Starting Shell ETL in directory: ${DATA_DIR}"
cd "${DATA_DIR}"

# 1. Unzip/extract archive if tolldata.tgz is present
if [ -f "tolldata.tgz" ]; then
    echo "Extracting tolldata.tgz..."
    tar -zxvf tolldata.tgz
fi

# 2. Extract data from CSV (vehicle-data.csv)
# Fields: Rowid, Timestamp, Anonymized Vehicle number, Vehicle type (fields 1 to 4)
echo "Extracting data from CSV..."
cut -d',' -f1-4 vehicle-data.csv > csv_data.csv

# 3. Extract data from TSV (tollplaza-data.tsv)
# Fields: Number of axles, Tollplaza id, Tollplaza code (fields 5 to 7)
echo "Extracting data from TSV..."
cut -f5-7 tollplaza-data.tsv | tr $'\t' ',' > tsv_data.csv

# 4. Extract data from fixed-width file (payment-data.txt)
# Fields: Type of Payment code, Vehicle Code (character positions 59-67)
echo "Extracting data from fixed-width file..."
cut -c 59-67 payment-data.txt | tr -s ' ' ',' | sed 's/^,//;s/,$//' > fixed_width_data.csv

# 5. Consolidate extracted data
# Combine columns from the three datasets into extracted_data.csv
echo "Consolidating extracted datasets into extracted_data.csv..."
paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv

# 6. Transform data
# Convert vehicle type (column 4) to uppercase and output to transformed_data.csv
echo "Transforming vehicle type to uppercase into transformed_data.csv..."
awk -F',' 'BEGIN {OFS=","} {$4=toupper($4); print}' extracted_data.csv > transformed_data.csv

echo "Shell ETL completed successfully. Output created: ${DATA_DIR}/transformed_data.csv"
