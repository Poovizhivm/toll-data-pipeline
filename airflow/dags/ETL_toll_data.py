from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments as per standard academic ETL requirements
default_args = {
    'owner': 'admin',
    'start_date': datetime(2026, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow DAG for Toll Data Batch ETL Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

DATA_DIR = "/opt/airflow/data"

# Task 1: Unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -zxvf {DATA_DIR}/tolldata.tgz -C {DATA_DIR}',
    dag=dag,
)

# Task 2: Extract data from CSV
# Extract: Rowid, Timestamp, Anonymized Vehicle number, Vehicle type (fields 1-4)
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f"cut -d',' -f1-4 {DATA_DIR}/vehicle-data.csv > {DATA_DIR}/csv_data.csv",
    dag=dag,
)

# Task 3: Extract data from TSV
# Extract: Number of axles, Tollplaza id, Tollplaza code (fields 5-7)
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"cut -f5-7 {DATA_DIR}/tollplaza-data.tsv | tr $'\\t' ',' > {DATA_DIR}/tsv_data.csv",
    dag=dag,
)

# Task 4: Extract data from fixed-width file
# Extract: Type of Payment code, Vehicle Code (positions 59-67)
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f"cut -c 59-67 {DATA_DIR}/payment-data.txt | tr -s ' ' ',' | sed 's/^,//;s/,$//' > {DATA_DIR}/fixed_width_data.csv",
    dag=dag,
)

# Task 5: Consolidate datasets
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f"paste -d',' {DATA_DIR}/csv_data.csv {DATA_DIR}/tsv_data.csv {DATA_DIR}/fixed_width_data.csv > {DATA_DIR}/extracted_data.csv",
    dag=dag,
)

# Task 6: Transform data (uppercase vehicle type field 4)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f"awk -F',' 'BEGIN {{OFS=\",\"}} {{$4=toupper($4); print}}' {DATA_DIR}/extracted_data.csv > {DATA_DIR}/transformed_data.csv",
    dag=dag,
)

# Task pipeline dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
