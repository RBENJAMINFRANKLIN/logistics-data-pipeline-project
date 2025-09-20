{% macro calculate_delay_percentage(delayed_flag_column) %}
    SUM(CASE WHEN {{ delayed_flag_column }} THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
{% endmacro %}