{% macro is_airport_trip(location_id) %}
    case
        when {{ location_id }} in (1, 132, 138)
        then true
        else false
    end
{% endmacro %}
