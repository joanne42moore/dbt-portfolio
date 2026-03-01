{% docs cast_dollars %}
Casts a value to numeric and rounds to 2 decimal places for consistent monetary formatting and to avoid floating-point errors. Pass the column or expression as the argument, e.g. `cast_dollars('sale_price')`.
{% enddocs %}

{% macro cast_dollars(expr) -%}
    round(cast({{ expr }} as numeric), 2)
{%- endmacro %}
