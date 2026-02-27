{% macro age_cohort_bucket(age) -%}
    case
        when {{ age }} < 18 then '<18'
        when {{ age }} < 25 then '18-24'
        when {{ age }} < 35 then '25-34'
        when {{ age }} < 45 then '35-44'
        when {{ age }} < 55 then '45-54'
        when {{ age }} < 65 then '55-64'
        else '65+'
    end
{%- endmacro %}

