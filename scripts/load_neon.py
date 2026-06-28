import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CRM_DB_URL = os.getenv("CRM_DB_URL")
ERP_DB_URL = os.getenv("ERP_DB_URL")

# ── CRM Tables ──────────────────────────────────────────────

CRM_TABLES = {
    "customer_info": "dataset/source_crm/cust_info.csv",
    "product_info":  "dataset/source_crm/prd_info.csv",
    "sales_info":    "dataset/source_crm/sales_details.csv",
}

# ── ERP Tables ──────────────────────────────────────────────

ERP_TABLES = {
    "erp_cust_az12":   "dataset/source_erp/CUST_AZ12.csv",
    "erp_loc_a101":    "dataset/source_erp/LOC_A101.csv",
    "erp_px_cat_g1v2": "dataset/source_erp/PX_CAT_G1V2.csv",
}


def load_csv_to_db(conn_str, tables: dict):
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    for table_name, csv_path in tables.items():
        print(f"Loading {csv_path} → {table_name}")
        df = pd.read_csv(csv_path)

        # Sanitize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Drop and recreate table
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        # Build CREATE TABLE from dataframe dtypes
        col_defs = []
        for col, dtype in zip(df.columns, df.dtypes):
            if "int" in str(dtype):
                pg_type = "BIGINT"
            elif "float" in str(dtype):
                pg_type = "FLOAT"
            else:
                pg_type = "TEXT"
            col_defs.append(f"{col} {pg_type}")

        create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)});"
        cur.execute(create_sql)

        # Insert rows
        for _, row in df.iterrows():
            values = tuple(None if pd.isna(v) else v for v in row)
            placeholders = ", ".join(["%s"] * len(values))
            cur.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", values)

        conn.commit()
        print(f"  ✓ {len(df)} rows loaded into {table_name}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    print("=== Loading CRM Database ===")
    load_csv_to_db(CRM_DB_URL, CRM_TABLES)

    print("\n=== Loading ERP Database ===")
    load_csv_to_db(ERP_DB_URL, ERP_TABLES)

    print("\nDone.")