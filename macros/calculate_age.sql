{% macro calculate_age(date_of_birth, as_of=none) %}
    {% if as_of is none %}
        {% set as_of_expr = "current_date" %}
    {% else %}
        {% set as_of_expr = as_of %}
    {% endif %}

  (
    datediff('year', {{ date_of_birth }}, {{ as_of_expr }}) 
    - 
    case 
        when dateadd('year', datediff('year', {{ date_of_birth }}, {{ as_of_expr }}), {{ date_of_birth }}) > {{ as_of_expr }}
        then 1 
        else 0 
    end
    )

{% endmacro %}