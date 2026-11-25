{#- A generic test you write once and use on any table:

        data_tests:
          - unique_together:
              arguments:
                columns: [store_id, till_id, business_date]

    It returns every combination of the columns that is there more than once. -#}
{% test unique_together(model, columns) %}

select {{ columns | join(', ') }}, count(*) as times
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
