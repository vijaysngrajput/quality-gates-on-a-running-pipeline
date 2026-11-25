{#- Use the schema a model or a test asks for, exactly. Without this, dbt would put the
    name of the default schema in front: `public_audit` in place of `audit`. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
