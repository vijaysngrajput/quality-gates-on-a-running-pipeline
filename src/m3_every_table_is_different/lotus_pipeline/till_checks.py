"""The checks for till_totals.

till_totals is the table nobody reads, and the one every money check leans on.
It is a CONTROL TOTAL: a total counted a second way, kept only to be compared.
A control you cannot trust is worse than none, so it gets three small checks of its own.
"""

from lotus_pipeline.checks import Check
from lotus_pipeline.sales_checks import EXPECTED_DAYS

CONTROL = "Everyone who is protected by the till total check: it is only as good as this table"

ONE_ROW_PER_TILL_PER_DAY = Check(
    number=14,
    key="one_total_per_till_per_day",
    family="the key is unique",
    name="A till has one total for a day",
    table="clean.till_totals",
    lens="data",
    question="Unique?",
    protects=CONTROL,
    prepare="Group by the key: store, till and day",
    limit="Each store_id + till_id + business_date is there exactly once",
    sql="""
        SELECT store_id, till_id, business_date AS sales_date, count(*) AS times
        FROM clean.till_totals
        GROUP BY store_id, till_id, business_date
        HAVING count(*) > 1
    """,
)

TILL_ADDS_UP = Check(
    number=15,
    key="till_adds_up",
    family="a row agrees with itself",
    name="A till's total adds up",
    table="clean.till_totals",
    lens="business",
    question="Consistent?",
    protects=CONTROL,
    prepare="Nothing. The three numbers are on one row",
    limit="net_total_cents is gross_sales_cents less refunds_cents. No number is below zero",
    sql="""
        SELECT store_id, till_id, business_date AS sales_date,
               gross_sales_cents, refunds_cents, net_total_cents
        FROM clean.till_totals
        WHERE net_total_cents <> gross_sales_cents - refunds_cents
           OR gross_sales_cents < 0 OR refunds_cents < 0
           OR net_total_cents IS NULL OR gross_sales_cents IS NULL OR refunds_cents IS NULL
    """,
)

EVERY_TILL_REPORTED = Check(
    number=16,
    key="every_till_reported",
    family="nothing is missing",
    name="Every till of an open store sent its total",
    table="clean.till_totals",
    lens="operational",
    question="Complete?",
    protects=CONTROL + ". A missing total would make a missing sales file look fine",
    prepare="Make the list of tills a day should have: two for every open store",
    limit="Each open store has a total for T1 and for T2 on each day",
    sql=f"""
        SELECT e.store_id, e.day AS sales_date, count(t.till_id) AS tills_reported
        FROM ({EXPECTED_DAYS}) e
        LEFT JOIN clean.till_totals t
          ON t.store_id = e.store_id AND t.business_date = e.day
        GROUP BY e.store_id, e.day
        HAVING count(t.till_id) <> 2
    """,
)

TILL_CHECKS = [ONE_ROW_PER_TILL_PER_DAY, TILL_ADDS_UP, EVERY_TILL_REPORTED]
