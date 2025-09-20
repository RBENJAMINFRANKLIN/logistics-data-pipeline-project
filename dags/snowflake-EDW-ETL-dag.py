from datetime import datetime
import os
import yaml
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Connection
from airflow import settings
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError
from utils.aws_utils import archive_and_clean_s3_folder
from utils.aws_utils import send_failure_sns_alert
from utils.check_files import skip_if_no_files

# Set your bucket and path
BUCKET = 'mar2025-training-bucket'
DATA_PREFIX = 'EDW/supply_sales/unprocessed/'  # only contents inside this
ARCHIVE_BASE_PREFIX = 'EDW/supply_sales/archive/'  # timestamped subfolder will be created here


def run_archive():
    archive_and_clean_s3_folder(BUCKET, DATA_PREFIX, ARCHIVE_BASE_PREFIX)



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': send_failure_sns_alert, 
}


with DAG(
    dag_id='snowflake_sales_etl_dag',
    default_args=default_args,
    description='A DAG to run multiple SQL queries in parallel in Snowflake',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    check_files = PythonOperator(
        task_id='check_stage_files',
        python_callable=skip_if_no_files
    )


# SELECT with TRY_CAST / IS_VALID

    # Task 1: sales bronze
    sales_bronze = SnowflakeOperator(
        task_id='sales_bronze',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""


CREATE OR REPLACE VIEW EDW.SALES_BRONZE.VW_SALES_BRONZE_VALID (
    SALES_ORDER_NUMBER,
    SALES_ORDER_LINENUMBER,
    ORDER_DATE,
    CUSTOMER_NAME,
    EMAIL,
    ITEM,
    QUANTITY,
    UNITPRICE,
    TAX,
    FILE_ROW_NUMBER,
    FILE_CONTENT_KEY,
    FILE_NAME,
    STG_MODIFIED_TS
) AS
SELECT 
    TRY_CAST($1 AS TEXT) AS SALES_ORDER_NUMBER,
    TRY_CAST($2 AS NUMBER(1, 0)) AS SALES_ORDER_LINENUMBER,
    TRY_CAST($3 AS DATE) AS ORDER_DATE,
    TRY_CAST($4 AS TEXT) AS CUSTOMER_NAME,
    TRY_CAST($5 AS TEXT) AS EMAIL,
    TRY_CAST($6 AS TEXT) AS ITEM,
    TRY_CAST($7 AS NUMBER(3, 0)) AS QUANTITY,
    TRY_CAST($8 AS NUMBER(8, 4)) AS UNITPRICE,
    TRY_CAST($9 AS NUMBER(8, 2)) AS TAX,
    metadata$FILE_ROW_NUMBER,
    metadata$FILE_CONTENT_KEY,
    metadata$FILENAME,
    metadata$FILE_LAST_MODIFIED
FROM @EDW.SALES_BRONZE.MY_AWS_STAGE/supply_sales/unprocessed/
(FILE_FORMAT => 'EDW.SALES_BRONZE.csv_with_header_sales', PATTERN => '[supply_sales]*.*csv') t
WHERE 
    TRY_CAST($1 AS TEXT) IS NOT NULL
    AND TRY_CAST($2 AS NUMBER(3, 0)) IS NOT NULL
    AND TRY_CAST($3 AS DATE) IS NOT NULL
    AND TRY_CAST($7 AS NUMBER(1, 0)) > 0
    AND TRY_CAST($8 AS NUMBER(8, 4)) >= 0  
    AND TRY_CAST($9 AS NUMBER(8, 2)) > 0;

    CREATE OR REPLACE VIEW EDW.SALES_BRONZE.VW_SALES_BRONZE_ERROR (
    RAW_SALES_ORDER_NUMBER,
    RAW_SALES_ORDER_LINENUMBER,
    RAW_ORDER_DATE,
    RAW_CUSTOMER_NAME,
    RAW_EMAIL,
    RAW_ITEM,
    RAW_QUANTITY,
    RAW_UNITPRICE,
    RAW_TAX,
    FILE_ROW_NUMBER,
    FILE_CONTENT_KEY,
    FILE_NAME,
    STG_MODIFIED_TS,
    ERROR_REASON
) AS
SELECT 
    $1 AS RAW_SALES_ORDER_NUMBER,
    $2 AS RAW_SALES_ORDER_LINENUMBER,
    $3 AS RAW_ORDER_DATE,
    $4 AS RAW_CUSTOMER_NAME,
    $5 AS RAW_EMAIL,
    $6 AS RAW_ITEM,
    $7 AS RAW_QUANTITY,
    $8 AS RAW_UNITPRICE,
    $9 AS RAW_TAX,
    metadata$FILE_ROW_NUMBER,
    metadata$FILE_CONTENT_KEY,
    metadata$FILENAME,
    metadata$FILE_LAST_MODIFIED,
    CASE 
        WHEN TRY_CAST($1 AS TEXT) IS NULL THEN 'Invalid SALES_ORDER_NUMBER'
        WHEN TRY_CAST($2 AS NUMBER(1, 0)) IS NULL THEN 'Invalid SALES_ORDER_LINENUMBER'
        WHEN TRY_CAST($3 AS DATE) IS NULL THEN 'Invalid ORDER_DATE'
        WHEN TRY_CAST($7 AS NUMBER(3, 0)) IS NULL THEN 'Invalid QUANTITY'
        WHEN TRY_CAST($8 AS NUMBER(8, 4)) IS NULL THEN 'Invalid UNITPRICE'
        WHEN TRY_CAST($9 AS NUMBER(8, 2)) IS NULL THEN 'Invalid TAX'
        ELSE 'Unknown Error'
    END AS ERROR_REASON
FROM @EDW.SALES_BRONZE.MY_AWS_STAGE/supply_sales/unprocessed/
(FILE_FORMAT => 'EDW.SALES_BRONZE.csv_with_header_sales', PATTERN => '[supply_sales]*.*csv') t
WHERE 
    TRY_CAST($1 AS TEXT) IS NULL
    OR TRY_CAST($2 AS NUMBER(1, 0)) IS NULL
    OR TRY_CAST($3 AS DATE) IS NULL
    OR TRY_CAST($7 AS NUMBER(1, 0)) IS NULL
    OR TRY_CAST($8 AS NUMBER(8, 4)) IS NULL
    OR TRY_CAST($9 AS NUMBER(8, 2)) IS NULL;

INSERT INTO EDW.SALES_BRONZE.SALES_BRONZE_RAW_AUDIT
SELECT 
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    metadata$FILE_ROW_NUMBER,
    metadata$FILE_CONTENT_KEY,
    metadata$FILENAME,
    metadata$FILE_LAST_MODIFIED,
    CURRENT_TIMESTAMP(),
    -- DQ Checks
    CASE 
        WHEN TRY_CAST($2 AS NUMBER(1,0)) IS NULL THEN FALSE
        WHEN TRY_CAST($3 AS DATE) IS NULL THEN FALSE
        WHEN TRY_CAST($7 AS NUMBER(1,0)) IS NULL THEN FALSE
        WHEN TRY_CAST($8 AS NUMBER(8,4)) IS NULL THEN FALSE
        WHEN TRY_CAST($9 AS NUMBER(8,2)) IS NULL THEN FALSE
        ELSE TRUE
    END AS IS_VALID_RECORD,
    CASE 
        WHEN TRY_CAST($2 AS NUMBER(1,0)) IS NULL THEN 'Invalid Line Number'
        WHEN TRY_CAST($3 AS DATE) IS NULL THEN 'Invalid Date'
        WHEN TRY_CAST($7 AS NUMBER(1,0)) IS NULL THEN 'Invalid Quantity'
        WHEN TRY_CAST($8 AS NUMBER(8,4)) IS NULL THEN 'Invalid Unit Price'
        WHEN TRY_CAST($9 AS NUMBER(8,2)) IS NULL THEN 'Invalid Tax' 
        ELSE NULL
    END AS ERROR_REASON
FROM @EDW.SALES_BRONZE.MY_AWS_STAGE/supply_sales/unprocessed/
(FILE_FORMAT => 'EDW.SALES_BRONZE.csv_with_header_sales', PATTERN => '.*\.csv');




        """,
    )

    # Task 2: sales_silver
    sales_silver = SnowflakeOperator(
        task_id='sales_silver',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""
   
-- Step 1 & 2: MERGE valid data into PROCESSED_SALES_DATA
MERGE INTO EDW.SALES_SILVER.PROCESSED_SALES_DATA AS target
USING (
    SELECT 
        SALES_ORDER_NUMBER,
        ORDER_DATE,
        EMAIL,
        ITEM,
        SALES_ORDER_LINENUMBER,
        CUSTOMER_NAME,
        QUANTITY,
        UNITPRICE,
        10 AS TAX,
        FILE_ROW_NUMBER,
        FILE_CONTENT_KEY,
        FILE_NAME,
        CURRENT_TIMESTAMP() AS CREATED_TS,
        CURRENT_TIMESTAMP() AS MODIFIED_TS,
        STG_MODIFIED_TS
    FROM EDW.SALES_BRONZE.VW_SALES_BRONZE_VALID
    WHERE 
        SALES_ORDER_NUMBER IS NOT NULL AND
        ORDER_DATE IS NOT NULL AND
        EMAIL ILIKE '%@%' AND
        ITEM IS NOT NULL AND
        QUANTITY > 0 AND
        UNITPRICE >= 0 AND
        CUSTOMER_NAME IS NOT NULL
) AS source
ON target.SALES_ORDER_NUMBER = source.SALES_ORDER_NUMBER
   AND target.ORDER_DATE = source.ORDER_DATE
   AND target.EMAIL = source.EMAIL
   AND target.ITEM = source.ITEM
WHEN MATCHED THEN
    UPDATE SET 
        target.SALES_ORDER_LINENUMBER = source.SALES_ORDER_LINENUMBER,
        target.EMAIL = source.EMAIL,
        target.QUANTITY = source.QUANTITY,
        target.UNITPRICE = source.UNITPRICE,
        target.TAX = source.TAX,
        target.MODIFIED_TS = source.MODIFIED_TS
WHEN NOT MATCHED THEN
    INSERT (
        SALES_ORDER_NUMBER,
        SALES_ORDER_LINENUMBER,
        ORDER_DATE,
        CUSTOMER_NAME,
        EMAIL,
        ITEM,
        QUANTITY,
        UNITPRICE,
        TAX,
        CREATED_TS,
        MODIFIED_TS,
        STG_MODIFIED_TS
    )
    VALUES (
        source.SALES_ORDER_NUMBER,
        source.SALES_ORDER_LINENUMBER,
        source.ORDER_DATE,
        source.CUSTOMER_NAME,
        source.EMAIL,
        source.ITEM,
        source.QUANTITY,
        source.UNITPRICE,
        source.TAX,
        source.CREATED_TS,
        source.MODIFIED_TS,
        source.STG_MODIFIED_TS
    );

-- Step 3: Insert invalid rows into error table
INSERT INTO EDW.SALES_SILVER.PROCESSED_SALES_DATA_ERROR (
    SALES_ORDER_NUMBER,
    SALES_ORDER_LINENUMBER,
    ORDER_DATE,
    CUSTOMER_NAME,
    EMAIL,
    ITEM,
    QUANTITY,
    UNITPRICE,
    TAX,
    FILE_ROW_NUMBER,
    FILE_CONTENT_KEY,
    FILE_NAME,
    CREATED_TS,
    MODIFIED_TS,
    STG_MODIFIED_TS,
    ERROR_REASON
)
SELECT 
    SALES_ORDER_NUMBER,
    SALES_ORDER_LINENUMBER,
    ORDER_DATE,
    CUSTOMER_NAME,
    EMAIL,
    ITEM,
    QUANTITY,
    UNITPRICE,
    TAX,
    FILE_ROW_NUMBER,
    FILE_CONTENT_KEY,
    FILE_NAME,
    CURRENT_TIMESTAMP() AS CREATED_TS,
    CURRENT_TIMESTAMP() AS MODIFIED_TS,
    STG_MODIFIED_TS,
    CASE 
        WHEN SALES_ORDER_NUMBER IS NULL THEN 'Missing Sales Order Number'
        WHEN ORDER_DATE IS NULL THEN 'Missing Order Date'
        WHEN EMAIL IS NULL OR EMAIL NOT ILIKE '%@%' THEN 'Invalid Email'
        WHEN ITEM IS NULL THEN 'Missing Item'
        WHEN QUANTITY IS NULL OR QUANTITY <= 0 THEN 'Invalid Quantity'
        WHEN UNITPRICE IS NULL OR UNITPRICE < 0 THEN 'Invalid Unit Price'
        WHEN CUSTOMER_NAME IS NULL THEN 'Missing Customer Name'
        ELSE NULL
    END AS ERROR_REASON
FROM EDW.SALES_BRONZE.VW_SALES_BRONZE_VALID
WHERE 
    SALES_ORDER_NUMBER IS NULL OR
    ORDER_DATE IS NULL OR
    EMAIL IS NULL OR EMAIL NOT ILIKE '%@%' OR
    ITEM IS NULL OR
    QUANTITY IS NULL OR QUANTITY <= 0 OR
    UNITPRICE IS NULL OR UNITPRICE < 0 OR
    CUSTOMER_NAME IS NULL;
        """,
    )

    # Task 3: Create dim_date
    dim_date = SnowflakeOperator(
        task_id='dim_date',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""

MERGE INTO EDW.SALES_SILVER.DIM_DATE AS target
USING  (
    SELECT 
        ORDER_DATE,
        REPLACE(TO_CHAR(ORDER_DATE, 'YYYY-MM-DD'), '-', '') AS ORDER_DATE_ID,
        YEAR(ORDER_DATE) AS YEAR,
        MONTH(ORDER_DATE) AS MONTH,
        DAY(ORDER_DATE) AS DAY,
        SUBSTRING(REPLACE(TO_CHAR(ORDER_DATE, 'YYYY-MM-DD'), '-', ''), 1, 6) AS YYYYMM
    FROM EDW.SALES_SILVER.PROCESSED_SALES_DATA
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_DATE ORDER BY ORDER_DATE) = 1 -- Drop duplicates by ORDER_DATE
) AS source
ON target.ORDER_DATE_ID = source.ORDER_DATE_ID
WHEN MATCHED THEN
    UPDATE SET
        target.ORDER_DATE = source.ORDER_DATE
WHEN NOT MATCHED THEN
    INSERT (ORDER_DATE_ID, ORDER_DATE, YEAR, MONTH, DAY, YYYYMM)
    VALUES (source.ORDER_DATE_ID, source.ORDER_DATE, source.YEAR, source.MONTH, source.DAY, source.YYYYMM);




        """,
    )

    # Task 4: Insert into dim_customer
    dim_customer = SnowflakeOperator(
        task_id='dim_customer',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""

MERGE INTO EDW.SALES_SILVER.DIM_CUSTOMER AS target
USING (
    SELECT 
        MAX(FS.CUSTOMER_NAME) AS CUSTOMER_NAME, 
        FS.EMAIL,
        CASE 
            WHEN COUNT(FS.SALES_ORDER_NUMBER) = 1 THEN 'New Customer'
            WHEN COUNT(FS.SALES_ORDER_NUMBER) > 1 THEN 'Returning Customer'
            WHEN SUM(FS.UNITPRICE * FS.QUANTITY + FS.TAX) > 10000 THEN 'VIP Customer'
            WHEN SUM(FS.QUANTITY) > 50 THEN 'Wholesale Buyer'
            WHEN MAX(FS.ORDER_DATE) < CURRENT_DATE - INTERVAL '180 days' THEN 'Dormant Customer'
            ELSE 'Regular Customer'
        END AS CUSTOMER_TYPE
    FROM EDW.SALES_SILVER.PROCESSED_SALES_DATA FS
    GROUP BY FS.EMAIL
) AS source
ON target.EMAIL = source.EMAIL

WHEN MATCHED THEN 
    UPDATE SET 
        target.CUSTOMER_NAME = source.CUSTOMER_NAME,
        target.CUSTOMER_TYPE = source.CUSTOMER_TYPE

WHEN NOT MATCHED THEN 
    INSERT (CUSTOMER_NAME, EMAIL, CUSTOMER_TYPE)
    VALUES (source.CUSTOMER_NAME, source.EMAIL, source.CUSTOMER_TYPE);
        """,
    )


    dim_product = SnowflakeOperator(
        task_id='dim_product',
        snowflake_conn_id='snowflakeid',retries=0,
        sql="""
-- Merge data from the source table into the target table
MERGE INTO EDW.SALES_SILVER.DIM_PRODUCT AS target
USING (
    SELECT DISTINCT ITEM AS ITEM_NAME,
        CASE 
            WHEN ITEM ILIKE '%Laptop%' OR ITEM ILIKE '%MacBook%' THEN 'Electronics'
            WHEN ITEM ILIKE '%Phone%' OR ITEM ILIKE '%iPhone%' OR ITEM ILIKE '%Samsung%' THEN 'Mobile Devices'
            WHEN ITEM ILIKE '%Shoes%' OR ITEM ILIKE '%Sneakers%' THEN 'Footwear'
            WHEN ITEM ILIKE '%T-shirt%' OR ITEM ILIKE '%Jacket%' OR ITEM ILIKE '%Jeans%' THEN 'Clothing'
            WHEN ITEM ILIKE '%Refrigerator%' OR ITEM ILIKE '%Microwave%' THEN 'Home Appliances'
            WHEN ITEM ILIKE '%Table%' OR ITEM ILIKE '%Chair%' THEN 'Furniture'
            WHEN ITEM ILIKE '%TV%' OR ITEM ILIKE '%Television%' THEN 'Entertainment'
            WHEN ITEM ILIKE '%Headphones%' OR ITEM ILIKE '%Earbuds%' THEN 'Accessories'
            ELSE 'Others' 
        END AS CATEGORY
    FROM EDW.SALES_SILVER.PROCESSED_SALES_DATA
) AS source
ON target.ITEM_NAME = source.ITEM_NAME

WHEN MATCHED THEN 
    UPDATE SET target.CATEGORY = source.CATEGORY

WHEN NOT MATCHED THEN 
    INSERT (ITEM_NAME, CATEGORY)
    VALUES (source.ITEM_NAME, source.CATEGORY);

        """,
    )


    fact_sales = SnowflakeOperator(
        task_id='fact_sales',
        snowflake_conn_id='snowflakeid',
        sql="""


-- Merge data from the source table into the target table

MERGE INTO EDW.SALES_SILVER.FACT_SALES AS target
USING (
    SELECT 
        FS.SALES_ORDER_NUMBER AS SALES_ORDER_NUMBER,
        NULLIF(DC.CUSTOMER_ID, -1) AS CUSTOMER_ID,
        NULLIF(DP.ITEM_ID, -1) AS ITEM_ID,
        NULLIF(DD.ORDER_DATE_ID, -1) AS ORDER_DATE_ID,
        FS.QUANTITY,
        FS.UNITPRICE,
        FS.TAX,
        (FS.QUANTITY * FS.UNITPRICE + FS.TAX) AS TOTAL_SALES_AMOUNT
    FROM EDW.SALES_SILVER.PROCESSED_SALES_DATA FS
    LEFT JOIN EDW.SALES_SILVER.DIM_CUSTOMER DC ON DC.EMAIL = FS.EMAIL
    LEFT JOIN EDW.SALES_SILVER.DIM_DATE DD ON FS.ORDER_DATE = DD.ORDER_DATE
    LEFT JOIN EDW.SALES_SILVER.DIM_PRODUCT DP ON DP.ITEM_NAME = FS.ITEM
) AS source
ON  target.SALES_ORDER_NUMBER = source.SALES_ORDER_NUMBER
 AND target.CUSTOMER_ID = source.CUSTOMER_ID
 AND target.ORDER_DATE_ID = source.ORDER_DATE_ID
 AND target.ITEM_ID = source.ITEM_ID
WHEN MATCHED THEN 
    UPDATE SET 
        target.QUANTITY = source.QUANTITY,
        target.UNIT_PRICE = source.UNITPRICE,
        target.TAX = source.TAX,
        target.TOTAL_SALES_AMOUNT = source.TOTAL_SALES_AMOUNT
WHEN NOT MATCHED THEN 
    INSERT (
        SALES_ORDER_NUMBER,
        CUSTOMER_ID,
        ITEM_ID,
        ORDER_DATE_ID,
        QUANTITY,
        UNIT_PRICE,
        TAX,
        TOTAL_SALES_AMOUNT
    ) 
    VALUES (
        source.SALES_ORDER_NUMBER,
        source.CUSTOMER_ID,
        source.ITEM_ID,
        source.ORDER_DATE_ID,
        source.QUANTITY,
        source.UNITPRICE,
        source.TAX,
        source.TOTAL_SALES_AMOUNT
    );



        """,
    )


    gold_customer_sales_agg = SnowflakeOperator(
        task_id='gold_customer_sales_agg',
        snowflake_conn_id='snowflakeid',
        sql="""   CREATE OR REPLACE TABLE EDW.SALES_GOLD.FACT_CUSTOMER_SALES_AGG AS 
                        SELECT 
                            c.CUSTOMER_ID,
                            c.CUSTOMER_NAME,
                            d.YEAR,
                            d.MONTH,
                            d.YYYYMM,
                            SUM(f.QUANTITY) AS TOTAL_QUANTITY,
                            SUM(f.TOTAL_SALES_AMOUNT) AS TOTAL_REVENUE,
                            COUNT(DISTINCT d.ORDER_DATE) AS ACTIVE_DAYS
                        FROM EDW.SALES_SILVER.FACT_SALES f
                        JOIN EDW.SALES_SILVER.DIM_CUSTOMER c ON f.CUSTOMER_ID = c.CUSTOMER_ID
                        JOIN EDW.SALES_SILVER.DIM_DATE d ON f.ORDER_DATE_ID = d.ORDER_DATE_ID
                        GROUP BY c.CUSTOMER_ID, c.CUSTOMER_NAME, d.YEAR, d.MONTH, d.YYYYMM;
     """,
        )
        
        
    gold_sales_summary_by_day = SnowflakeOperator(
        task_id='gold_sales_summary_by_day',
        snowflake_conn_id='snowflakeid',
        sql="""  CREATE OR REPLACE  TABLE EDW.SALES_GOLD.sales_summary_by_day AS
                SELECT 
                    d.ORDER_DATE,
                    SUM(fs.TOTAL_SALES_AMOUNT) AS total_sales,
                    SUM(fs.QUANTITY) AS total_units_sold,
                    COUNT(DISTINCT fs.SALES_ORDER_NUMBER) AS total_orders
                FROM EDW.SALES_SILVER.FACT_SALES fs
                JOIN EDW.SALES_SILVER.DIM_DATE d ON fs.ORDER_DATE_ID = d.ORDER_DATE_ID
                GROUP BY d.ORDER_DATE;

     """,
        )
        
        
    gold_product_sales_summary = SnowflakeOperator(
        task_id='gold_product_sales_summary',
        snowflake_conn_id='snowflakeid',
        sql="""  CREATE OR REPLACE TABLE EDW.SALES_GOLD.product_sales_summary AS
            SELECT 
                p.ITEM_ID,
                p.ITEM_NAME,
                p.CATEGORY,
                SUM(fs.QUANTITY) AS units_sold,
                SUM(fs.TOTAL_SALES_AMOUNT) AS revenue,
                COUNT(DISTINCT fs.SALES_ORDER_NUMBER) AS order_count
            FROM EDW.SALES_SILVER.FACT_SALES fs
            JOIN EDW.SALES_SILVER.DIM_PRODUCT p ON fs.ITEM_ID = p.ITEM_ID
            GROUP BY p.ITEM_ID, p.ITEM_NAME, p.CATEGORY;

     """,
        )
    

    archive_task = PythonOperator(
        task_id='archive_s3_files',
        python_callable=run_archive
    )



    #sales_bronze >> sales_silver >> dim_date >> dim_customer >> dim_product >> fact_sales >> gold_customer_sales_agg >> gold_sales_summary_by_day >> gold_product_sales_summary




    # Task Dependencies
    check_files >> sales_bronze >> sales_silver

sales_silver >> dim_date
sales_silver >> dim_customer
sales_silver >> dim_product

[dim_date, dim_customer, dim_product] >> fact_sales

fact_sales >> gold_customer_sales_agg
fact_sales >> gold_sales_summary_by_day
fact_sales >> gold_product_sales_summary

[gold_customer_sales_agg,gold_sales_summary_by_day,gold_product_sales_summary] >> archive_task