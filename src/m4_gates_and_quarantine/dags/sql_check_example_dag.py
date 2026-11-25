"""One check, written a second way: with Airflow's own SQL check operator.

In this project the gates run dbt tests. At work you will also meet this shape: the SQL
of the check lives in the DAG file, and one Airflow operator runs it.

    SQLCheckOperator runs a query that returns ONE ROW.
    The check passes when every value in that row is true (or not zero).

Compare it with dbt_project/tests/clean/refund_gives_money_back.sql. It is the same rule.

    dbt test                       SQLCheckOperator
    returns the bad rows           returns one true-or-false row
    passes when no row is back     passes when the row is true
    the rule lives in dbt          the rule lives in the DAG file
    can keep the bad rows          cannot: it only knows pass or fail

Start it by hand on the DAG's page, or:
    docker compose exec airflow airflow dags trigger lotus_sql_check_example_m4
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.sdk import DAG

with DAG(
    dag_id="lotus_sql_check_example_m4",
    description="A lab: the refund rule, as an Airflow SQL check in place of a dbt test.",
    schedule=None,  # it never runs by itself
    start_date=datetime(2026, 7, 27),
    catchup=False,
    tags=["lotus", "milestone 4", "lab"],
) as dag:
    # raw.sales_lines keeps every row as it arrived, so the planted refund is still there.
    refund_gives_money_back = SQLCheckOperator(
        task_id="refund_gives_money_back",
        conn_id="lotus_warehouse",  # set in docker-compose.yml, as AIRFLOW_CONN_LOTUS_WAREHOUSE
        database="lotus_m4",
        sql="""
            SELECT count(*) = 0
            FROM raw.sales_lines
            WHERE line_type = 'refund' AND CAST(amount_cents AS BIGINT) >= 0
        """,
    )
