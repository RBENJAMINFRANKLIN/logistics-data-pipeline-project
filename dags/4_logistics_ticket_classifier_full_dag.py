from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from typing import Optional
import yaml
from utils.aws_utils import send_failure_sns_alert
import boto3
import pandas as pd
from snowflake.connector import connect
import yaml


# 🔹 Load Snowflake config
def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']

# 🔹 Create Snowflake session
def create_snowflake_session():
    config = load_snowflake_config()
    return snowpark.Session.builder.configs(config).create()

# 🔹 ML Training
training_data = [
    ("I want to cancel my order", "Order Cancellation"),
    ("The payment failed via UPI", "Payment Issue"),
    ("My product is delayed by 3 days", "Delivery Issue"),
    ("I need to return the item", "Return Request"),
    ("When does the sale start?", "General Inquiry"),
    ("Still waiting for delivery update", "Delivery Issue"),
    ("I paid but didn’t get confirmation", "Payment Issue"),
    ("Want to exchange the item", "Return Request"),
    ("How to track my order?", "General Inquiry"),
    ("Please cancel immediately", "Order Cancellation")
]

X_train = [text for text, label in training_data]
y_train = [label for text, label in training_data]

pipeline = Pipeline([
    ('tfidf', TfidfVectorizer()),
    ('clf', LogisticRegression(max_iter=200))
])
pipeline.fit(X_train, y_train)




from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType
from typing import Optional

def classify_and_log(**context):
    session = create_snowflake_session()

    # Read table from Snowflake to Pandas
    df = session.table("LOGISTICS_DEMO_1.SILVER.customer_support_flat").to_pandas()

    # Apply classification locally using sklearn pipeline
    df["TICKET_CATEGORY"] = df["MESSAGE_TEXT"].apply(lambda x: pipeline.predict([x])[0] if x else "Unknown")

    # Write enriched data to a Snowflake GOLD table
    session.write_pandas(
        df,
        table_name="CUSTOMER_SUPPORT_ENRICHED",
        database="LOGISTICS_DEMO_1",
        schema="GOLD",
        auto_create_table=True,
        overwrite=True
    )


# Load Snowflake config from YAML
def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']

# Task 1: Fetch data from Snowflake
def get_data_from_snowflake():
    config = load_snowflake_config()
    conn = connect(**config)
    cursor = conn.cursor()
    query = """
        SELECT TICKET_ID, CUSTOMER_ID, MESSAGE_TEXT, ticket_category
        FROM LOGISTICS_DEMO_1.GOLD.CUSTOMER_SUPPORT_ENRICHED
        LIMIT 10
    """
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()

    df.to_csv('/tmp/customer_support_details.csv', index=False)
    return df.to_string(index=False)

# Task 2: Send email using AWS SES
def send_email_with_data():
    df = pd.read_csv('/tmp/customer_support_details.csv')
    # Convert DataFrame to HTML table
    html_table = df.to_html(index=False, border=1, justify='center', classes='data-table')
        # Optional: Add custom HTML styling
    html_body = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                font-size: 14px;
            }}
            .data-table {{
                border-collapse: collapse;
                width: 100%;
            }}
            .data-table th, .data-table td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
            }}
            .data-table th {{
                background-color: #f2f2f2;
                color: #333;
            }}
        </style>
    </head>
    <body>
        <p>Hi,</p>
        <p>Here is the Customer Support Data:</p>
        {html_table}
        <p>Thanks,<br>Data Team</p>
    </body>
    </html>
    """
    client = boto3.client('ses', region_name="ap-south-1")
    client.send_email(
        Source="benjaminimp10@gmail.com",
        Destination={'ToAddresses': ["velms2024@gmail.com"]},
        Message={
            'Subject': {'Data': 'Customer Support Data '},
            'Body': {
                'Html': {'Data': html_body}
            }})    

# 🔹 DAG defaults
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'on_failure_callback': send_failure_sns_alert, 
}

# 🔹 DAG Definition
with DAG(
    dag_id='4_logistics_ticket_classifier_full_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'logistics', 'ml'],
) as dag:

    # Step 1: Copy JSON from S3
    copy_into_task = SnowflakeOperator(
        task_id="copy_s3_to_snowflake",
        snowflake_conn_id='snowflakeid',
        sql="""
            COPY INTO LOGISTICS_DEMO_1.BRONZE.customer_support_raw 
            FROM @LOGISTICS_DEMO_1.BRONZE.AWS_DEMO_STAGE_3/logistics_customer_support_json/2025-09-11/shiprocket_support_tickets.json
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'SKIP_FILE';
        """,
    )

    # Step 2: Flatten JSON into structured table
    flatten_insert_task = SnowflakeOperator(
        task_id="flatten_and_insert",
        snowflake_conn_id='snowflakeid',
        sql="""
            INSERT INTO LOGISTICS_DEMO_1.SILVER.customer_support_flat (
                ticket_id,
                customer_id,
                message_text,
                created_at
            )
            SELECT value:"ticket_id"::STRING AS ticket_id,
                value:"customer":"id"::STRING AS customer_id,
                value:"message":"body"::STRING AS message_text,
                value:"created_at"::TIMESTAMP AS created_at
                FROM LOGISTICS_DEMO_1.BRONZE.customer_support_raw,
                LATERAL FLATTEN(input => json_data);
        """,
    )


    # Step 4: Apply classifier to enrich data
    classify_tickets = PythonOperator(
        task_id='apply_classifier_to_tickets',
        python_callable=classify_and_log
    )

    fetch_data = PythonOperator(
        task_id='fetch_snowflake_data',
        python_callable=get_data_from_snowflake,
        dag=dag,
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email_with_data,
        dag=dag,
    )


    # DAG Task Flow
    copy_into_task >> flatten_insert_task  >> classify_tickets >> fetch_data >> send_email
