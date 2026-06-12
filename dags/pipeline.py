import psycopg2
import pandas as pd
import requests
from datetime import datetime, timedelta

# Import Airflow dependencies
from airflow import DAG
from airflow.operators.python import PythonOperator

# Database Configuration (Note: host is now 'postgres_warehouse' because it runs inside Docker network)
conn_params = {
    "host": "postgres_warehouse",
    "database": "local_dw",
    "user": "de_user",
    "password": "adminpassword",
    "port": "5432"
}

def run_crypto_pipeline():
    """The unified ELT pipeline function."""
    # 1. EXTRACT
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code}")
    raw_data = response.json()
    
    # 2. TRANSFORM
    processed_records = []
    current_time = datetime.now()
    for coin_id, metrics in raw_data.items():
        processed_records.append({
            "coin_id": coin_id,
            "price_usd": metrics["usd"],
            "market_cap_usd": metrics["usd_market_cap"],
            "volume_24h_usd": metrics["usd_24h_vol"],
            "extracted_at": current_time
        })
    df = pd.DataFrame(processed_records)
    
    # 3. LOAD
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_metrics_staging (
            coin_id VARCHAR(50),
            price_usd NUMERIC(16, 4),
            market_cap_usd NUMERIC(20, 2),
            volume_24h_usd NUMERIC(20, 2),
            extracted_at TIMESTAMP,
            PRIMARY KEY (coin_id, extracted_at)
        );
    """)
    conn.commit()
    
    insert_query = """
        INSERT INTO market_metrics_staging (coin_id, price_usd, market_cap_usd, volume_24h_usd, extracted_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (coin_id, extracted_at) DO NOTHING;
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['coin_id'], float(row['price_usd']), float(row['market_cap_usd']), float(row['volume_24h_usd']), row['extracted_at']
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print("🎯 Pipeline executed smoothly via Airflow!")


# --- DEFINE THE AIRFLOW WORKFLOW ARCHITECTURE ---
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_extraction_pipeline',
    default_args=default_args,
    description='Automated pipeline to fetch daily crypto metrics',
    schedule_interval='@daily', # Can be changed to cron expressions like '*/5 * * * *' for every 5 mins
    catchup=False
) as dag:

    # Define the single execution task node
    execute_pipeline_task = PythonOperator(
        task_id='fetch_and_load_crypto_data',
        python_callable=run_crypto_pipeline
    )