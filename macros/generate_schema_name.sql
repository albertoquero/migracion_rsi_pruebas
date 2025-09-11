{% macro generate_schema_name(custom_schema_name, node) %}
    {# Si se especifica un schema en el modelo, úsalo #}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name }}
    {% else %}
        {# Cambia "mi_schema_por_defecto" por el schema base que quieras #}
        mi_schema_por_defecto
    {% endif %}
{% endmacro %}