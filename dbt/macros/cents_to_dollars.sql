{% macro cents_to_dollars(amount) %}
    round({{ amount }} / 100.0, 2)
{% endmacro %}
