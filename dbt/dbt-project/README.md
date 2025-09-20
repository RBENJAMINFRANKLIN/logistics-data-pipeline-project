Welcome to your new dbt project!

### Using the starter project

Try running the following commands:


- dbt clean
-  dbt deps
- dbt run
- dbt test
- dbt snapshot 


dbt test --select source:LOGISTICS_RAW.orders

dbt test --select test__delivery_performance_by_courier




### 🧩 Optional: Include dependencies (like silver models)
dbt run --select +path:models/silver/


### ✅ Option 1: Use Model Names
dbt run --select model_a model_b


### ✅ Option 2: Use Specific File Paths

dbt run --select path:models/gold/mart_sales.sql path:models/gold/mart_customers.sql


dbt run --select +path:models/silver/dim_customers.sql

dbt run --select delivery_sla_rootcause       
dbt test --select test__delivery_sla_rootcause

dbt run --select seller_product_profitability   
dbt test --select test__seller_product_profitability

dbt seed --select gold.control_table