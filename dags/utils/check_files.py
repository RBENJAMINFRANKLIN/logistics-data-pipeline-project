# File: scripts/check_delayed_orders.py

import os
import yaml
import snowflake.connector
import sys
from airflow.exceptions import AirflowSkipException


os.environ["SF_OCSP_FAIL_OPEN"] = "true"
os.environ["SF_OCSP_TESTING_ENDPOINT"] = "http://127.0.0.1:12345"

def load_snowflake_config():
    config_path = os.path.join(os.path.dirname(__file__), '../utils/snowflake_config.yaml')
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['snowflake']

def skip_if_no_files():
    sf_config = load_snowflake_config()
    try:
        conn = snowflake.connector.connect(
            user=sf_config["user"],
            password=sf_config["password"],
            account=sf_config["account"],
            warehouse=sf_config["warehouse"],
            database=sf_config["database"],
            schema=sf_config["schema"],
            role=sf_config["role"],
            login_timeout=20,
            client_session_keep_alive=False,
            ocsp_fail_open=True
        )
        print("✅ Connected to Snowflake")

        cursor = conn.cursor()
        cursor.execute("LIST @EDW.SALES_BRONZE.MY_AWS_STAGE/supply_sales/unprocessed/ PATTERN='.*\\.csv';")
        if not cursor.fetchall():
            raise AirflowSkipException("No files to process.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error:", e)
        sys.exit(1)  # non-zero exit = task fail


if __name__ == "__main__":
    skip_if_no_files()
