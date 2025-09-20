{% macro log_audit_start(model_name, source_name, extracted_at_column) %}
INSERT INTO LOGISTICS_DEMO_1.SILVER.ETL_AUDIT_LOG (
    JOB_NAME,
    RUN_ID,
    STATUS,
    START_TIME,
    END_TIME,
    ROWS_PROCESSED
)
SELECT
    '{{ model_name }}',
    '{{ invocation_id }}',
    'STARTED',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    COUNT(*)
FROM {{ source_name }}
WHERE {{ extracted_at_column }} > COALESCE(
    (SELECT MAX({{ extracted_at_column }}) FROM {{ this }}), '2000-01-01'
)
{% endmacro %}
