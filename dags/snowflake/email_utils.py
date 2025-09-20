from datetime import datetime, timedelta
from snowflake.connector import connect
import pandas as pd
import boto3
import yaml
from botocore.exceptions import ClientError


# SES Config
SENDER_EMAIL = "benjaminimp10@gmail.com"
RECEIVER_EMAIL = "velms2024@gmail.com"
REGION = "ap-south-1"

# Snowflake Query
QUERY = """
SELECT * FROM retail_demo_db.gold.retail_sales_summary_daily
WHERE SALES_DATE = CURRENT_DATE();
"""

# 1️⃣ Generate the HTML report from DataFrame
def generate_sales_report_html(df: pd.DataFrame) -> str:
    date = df['SALES_DATE'].iloc[0]
    total_txns = df['TOTAL_TRANSACTIONS'].sum()
    total_items = df['TOTAL_ITEMS_SOLD'].sum()
    gross_sales = df['GROSS_SALES'].sum()
    refunded = df['TOTAL_REFUNDED'].sum()
    net_sales = df['NET_SALES'].sum()

    top_stores = df.sort_values('GROSS_SALES', ascending=False).head(5)
    df['REFUND_PCT'] = (df['TOTAL_REFUNDED'] / df['GROSS_SALES']) * 100
    refund_stores = df.sort_values('REFUND_PCT', ascending=False).head(5)

    city_summary = df.groupby('CITY').agg({
        'TOTAL_TRANSACTIONS': 'sum',
        'GROSS_SALES': 'sum',
        'TOTAL_REFUNDED': 'sum',
        'NET_SALES': 'sum'
    }).reset_index()

    html = f"""
    <html>
    <head>
    <style>
    table {{ border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ background-color: #f2f2f2; }}
    </style>
    </head>
    <body>
    <h2>📊 Retail Sales Daily Report – {date}</h2>
    <h3>🔹 Executive Summary</h3>
    <table>
        <tr><th>Total Stores</th><td>{len(df)}</td></tr>
        <tr><th>Total Transactions</th><td>{total_txns}</td></tr>
        <tr><th>Total Items Sold</th><td>{total_items}</td></tr>
        <tr><th>Gross Sales</th><td>₹{gross_sales:,.2f}</td></tr>
        <tr><th>Total Refunded</th><td>₹{refunded:,.2f}</td></tr>
        <tr><th>Net Sales</th><td>₹{net_sales:,.2f}</td></tr>
    </table>

    <h3>🏆 Top 5 Stores by Gross Sales</h3>
    {top_stores[['STORE_ID', 'CITY', 'GROSS_SALES', 'NET_SALES']].to_html(index=False)}

    <h3>⚠️ Stores with Highest Refund %</h3>
    {refund_stores[['STORE_ID', 'CITY', 'GROSS_SALES', 'TOTAL_REFUNDED', 'REFUND_PCT']].to_html(index=False, float_format="%.2f")}

    <h3>🌆 Sales Breakdown by City</h3>
    {city_summary.to_html(index=False, float_format="%.2f")}

    <br><p>Regards,<br><b>Data Engineering Team</b></p>
    </body>
    </html>
    """
    return html





# 2️⃣ Send email
def send_email_via_ses(subject: str, body_html: str):
    ses = boto3.client('ses', region_name=REGION)
    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [RECEIVER_EMAIL]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Html': {'Data': body_html}}
            }
        )
        print("✅ Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("❌ Email failed:", e.response['Error']['Message'])
        raise


def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']


# 3️⃣ Load data, generate report, send email
def query_snowflake_and_send_report():
    config = load_snowflake_config()
    conn = connect(**config)
    cursor = conn.cursor()
    print("🔗 Connected to Snowflake")
    print("🔍 Executing query..." + QUERY)
    cursor.execute(QUERY)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    if df.empty:
        send_email_via_ses("❌ No Data for Today", "<p>No data available for today.</p>")
        return

    report_html = generate_sales_report_html(df)



    
    report_date = df['SALES_DATE'].iloc[0]
    send_email_via_ses(f"📩 Daily Retail Sales Report – {report_date}", report_html)

