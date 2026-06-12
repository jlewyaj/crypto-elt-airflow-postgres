import psycopg2
import pandas as pd

# 1. Define connection parameters to our Docker container
conn_params = {
    "host": "localhost",
    "database": "local_dw",
    "user": "de_user",
    "password": "adminpassword",
    "port": "5432"
}

def test_pipeline():
    # 2. Simulate extracted data (e.g., sample e-commerce transactions)
    mock_data = [
        {"tx_id": 101, "customer": "Alice", "amount": 250.50, "status": "Completed"},
        {"tx_id": 102, "customer": "Bob", "amount": 89.00, "status": "Pending"},
        {"tx_id": 103, "customer": "Charlie", "amount": 1200.00, "status": "Completed"}
    ]
    df = pd.DataFrame(mock_data)
    print("--- Data Extracted Successfully ---")
    print(df)

    # 3. Connect to the local Postgres Warehouse
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create a staging table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_transactions (
                tx_id INT PRIMARY KEY,
                customer VARCHAR(100),
                amount NUMERIC(10, 2),
                status VARCHAR(50)
            );
        """)
        conn.commit()
        
        # Insert dataframe rows into table
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO staging_transactions (tx_id, customer, amount, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tx_id) DO NOTHING;
            """, (int(row['tx_id']), row['customer'], float(row['amount']), row['status']))
        
        conn.commit()
        print("\n--- Data Successfully Loaded into Local Postgres Container! ---")
        
        # Verify the data is inside the DB
        cursor.execute("SELECT COUNT(*) FROM staging_transactions;")
        print(f"Total rows in DB: {cursor.fetchone()[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    test_pipeline()