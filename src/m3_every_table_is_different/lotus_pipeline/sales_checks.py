"""The checks for sales_lines, each with a reason.

Milestone 1 ended with a check card and a list of worries. This file turns the worries
into checks. Every check here can answer the question the five standard checks could not:
who does it protect?

A check is still a query that returns the bad rows. What is new is the work BEFORE the
rule: most of these queries join, add up or compare first. Each check looks through one
of three lenses:

    data         the values themselves: empty, twice, wrong shape
    business     a rule of how Lotus works: a refund gives money back
    operational  the pipeline: did every store send a file, with a normal amount of data

The checks read the whole table, not one night. A bad row stays a bad row until someone
fixes it or sets it aside.
"""

from lotus_pipeline.checks import Check

MANAGERS = "Store managers and the head of retail operations: net sales, so pay"
EVERYONE = "Everyone who reads the two outputs"

# The newest business day the pipeline has loaded: the day before its newest run.
LAST_DAY = "(SELECT CAST(max(_loaded_at) AS DATE) - 1 FROM raw.sales_lines)"

# How far a store's day may be from its normal day before we call it strange.
VOLUME_LOW, VOLUME_HIGH = 0.6, 1.4
# Nobody carries more than this many kilograms of one product to a till.
MAX_KILOGRAMS = 20

# ------------------------------------------------------------------ data lens

REQUIRED_CELLS = Check(
    number=1,
    key="required_cells",
    family="cells are filled",
    name="A cell that must be filled is filled",
    table="clean.sales_lines",
    lens="data",
    question="Complete?",
    protects=MANAGERS,
    prepare="Decide, column by column, in which rows an empty cell is wrong",
    limit="No empty amount, quantity, price, time, product or receipt. "
    "A refund names its receipt, and a sale does not",
    sql="""
        SELECT line_id, store_id, sales_date, 'a cell that must be filled is empty' AS problem
        FROM clean.sales_lines
        WHERE receipt_id IS NULL OR line_no IS NULL OR sold_at IS NULL OR product_id IS NULL
           OR quantity IS NULL OR unit_price_cents IS NULL OR amount_cents IS NULL
        UNION ALL
        SELECT line_id, store_id, sales_date, 'a refund that names no receipt' AS problem
        FROM clean.sales_lines
        WHERE line_type = 'refund' AND refund_of_receipt_id IS NULL
        UNION ALL
        SELECT line_id, store_id, sales_date, 'a sale that names a refunded receipt' AS problem
        FROM clean.sales_lines
        WHERE line_type = 'sale' AND refund_of_receipt_id IS NOT NULL
    """,
)

TRUE_KEY_UNIQUE = Check(
    number=2,
    key="true_key_unique",
    family="the key is unique",
    name="One item line is stored once",
    table="clean.sales_lines",
    lens="data",
    question="Unique?",
    protects=MANAGERS,
    prepare="Group by the key that means something: receipt_id + line_no, not line_id",
    limit="Each receipt_id + line_no is there exactly once",
    catches=(2, 9),
    sql="""
        SELECT receipt_id, line_no, store_id, min(sales_date) AS sales_date, count(*) AS times,
               sum(amount_cents) - min(amount_cents) AS counted_too_much_cents
        FROM clean.sales_lines
        GROUP BY receipt_id, line_no, store_id
        HAVING count(*) > 1
    """,
)

RIGHT_SHAPE = Check(
    number=3,
    key="right_shape",
    family="values are allowed",
    name="A receipt id has the right shape",
    table="clean.sales_lines",
    lens="data",
    question="Valid?",
    protects="Finance and the stores: a refund must find the receipt it refunds",
    prepare="Take the id apart: store, till, six digits",
    limit="receipt_id is the line's own store, its own till, and six digits: S03-T1-004493",
    sql="""
        SELECT line_id, store_id, sales_date, receipt_id
        FROM clean.sales_lines
        WHERE length(receipt_id) <> 13
           OR substr(receipt_id, 1, 7) <> store_id || '-' || till_id || '-'
           OR translate(substr(receipt_id, 8, 6), '0123456789', '') <> ''
    """,
)

LINE_ADDS_UP = Check(
    number=4,
    key="line_adds_up",
    family="a row agrees with itself",
    name="A line adds up",
    table="clean.sales_lines",
    lens="data",
    question="Consistent?",
    protects=MANAGERS,
    prepare="Work the amount out again from the line's own quantity and price",
    limit="amount_cents is quantity x unit_price_cents, to the cent",
    catches=(1,),
    sql="""
        SELECT line_id, store_id, sales_date, quantity, unit_price_cents, amount_cents,
               CAST(round(quantity * unit_price_cents) AS BIGINT) AS should_be_cents
        FROM clean.sales_lines
        WHERE amount_cents <> CAST(round(quantity * unit_price_cents) AS BIGINT)
    """,
)

EVERY_SALE_HAS_A_PRODUCT = Check(
    number=5,
    key="every_sale_has_a_product",
    family="it points at a row that exists",
    name="Every sale is for a product that exists",
    table="raw.sales_lines",
    lens="data",
    question="Consistent?",
    protects=EVERYONE + ": a line with no product is dropped, and no row is left to look at",
    prepare="Go back to the raw rows, and join each to the product valid on its day",
    limit="No raw sales line is without a product or a store",
    catches=(6,),
    sql="""
        SELECT l.line_id, l.store_id, CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE) AS sales_date,
               l.product_id, CAST(l.amount_cents AS BIGINT) AS lost_cents
        FROM raw.sales_lines l
        LEFT JOIN clean.products p
          ON p.product_id = l.product_id
         AND CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE)
             BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
        LEFT JOIN clean.stores s ON s.store_id = l.store_id
        WHERE p.product_id IS NULL OR s.store_id IS NULL
    """,
)

# ------------------------------------------------------------------ business lens

REFUND_GIVES_MONEY_BACK = Check(
    number=6,
    key="refund_gives_money_back",
    family="values are allowed",
    name="A refund gives money back",
    table="clean.sales_lines",
    lens="business",
    question="Valid?",
    protects=MANAGERS,
    prepare="Read sales and refunds apart",
    limit="A refund has a quantity and an amount below zero. A sale has both above zero",
    catches=(1,),
    sql="""
        SELECT line_id, store_id, sales_date, line_type, quantity, amount_cents
        FROM clean.sales_lines
        WHERE (line_type = 'refund' AND (amount_cents >= 0 OR quantity >= 0))
           OR (line_type = 'sale' AND (amount_cents <= 0 OR quantity <= 0))
    """,
)

REFUND_NOT_LARGER_THAN_SALE = Check(
    number=7,
    key="refund_not_larger_than_sale",
    family="two tables agree",
    name="A refund is not larger than the sale",
    table="clean.sales_lines",
    lens="business",
    question="Consistent?",
    protects="Finance: money given back for something that was never sold",
    prepare="Join each refund to the receipt it names, and add up what that receipt "
    "sold of the same product",
    limit="The receipt exists, and the money given back is not more than it took",
    sql="""
        SELECT r.line_id, r.store_id, r.sales_date, r.refund_of_receipt_id,
               abs(r.amount_cents) AS given_back_cents, sold.amount_cents AS sold_cents
        FROM clean.sales_lines r
        LEFT JOIN (
            SELECT receipt_id, product_id, sum(amount_cents) AS amount_cents
            FROM clean.sales_lines
            WHERE line_type = 'sale'
            GROUP BY receipt_id, product_id
        ) sold
          ON sold.receipt_id = r.refund_of_receipt_id AND sold.product_id = r.product_id
        WHERE r.line_type = 'refund'
          AND (sold.receipt_id IS NULL OR abs(r.amount_cents) > sold.amount_cents)
    """,
)

PRICE_OF_THE_DAY = Check(
    number=8,
    key="price_of_the_day",
    family="two tables agree",
    name="The price charged is the price valid that day",
    table="clean.sales_lines",
    lens="business",
    question="Accurate?",
    protects="Finance and the store: money the store should have taken and did not",
    prepare="Join each line to the product's price row that is valid on the day of the sale",
    limit="unit_price_cents equals the price list, exactly",
    catches=(4,),
    sql="""
        SELECT l.line_id, l.store_id, l.sales_date, l.product_name, l.quantity,
               l.unit_price_cents AS charged_cents, p.unit_price_cents AS price_list_cents,
               CAST(round(l.quantity * (p.unit_price_cents - l.unit_price_cents)) AS BIGINT)
                   AS store_short_cents
        FROM clean.sales_lines l
        JOIN clean.products p
          ON p.product_id = l.product_id
         AND l.sales_date BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
        WHERE l.unit_price_cents <> p.unit_price_cents
    """,
)

INSIDE_OPENING_HOURS = Check(
    number=9,
    key="inside_opening_hours",
    family="two tables agree",
    name="Nothing is sold while the store is closed",
    table="clean.sales_lines",
    lens="business",
    question="Valid?",
    protects=MANAGERS + ". A wrong time can be a wrong day, and a wrong week",
    prepare="Join each line to its store for the opening hours, and to the calendar "
    "for a day with other hours",
    limit="sold_at is between opens_at and closes_at of that store on that day",
    catches=(5,),
    sql="""
        SELECT l.line_id, l.store_id, l.sales_date, l.sold_at,
               coalesce(c.opens_at, s.opens_at) AS opens_at,
               coalesce(c.closes_at, s.closes_at) AS closes_at, l.amount_cents
        FROM clean.sales_lines l
        JOIN clean.stores s ON s.store_id = l.store_id
        LEFT JOIN clean.calendar c
          ON c.store_id = l.store_id AND c.calendar_date = l.sales_date
        WHERE CAST(l.sold_at AS TIME) < coalesce(c.opens_at, s.opens_at)
           OR CAST(l.sold_at AS TIME) > coalesce(c.closes_at, s.closes_at)
    """,
)

BELIEVABLE_QUANTITY = Check(
    number=10,
    key="believable_quantity",
    family="values are allowed",
    name="A weight is a believable weight",
    table="clean.sales_lines",
    lens="business",
    question="Valid?",
    protects=MANAGERS,
    prepare="Join each line to its product for the unit: pieces or kilograms",
    limit=f"Goods sold by weight: not more than {MAX_KILOGRAMS} kg on one line. "
    "Goods sold by the piece: a whole number",
    catches=(3,),
    sql=f"""
        SELECT line_id, store_id, sales_date, product_name, unit, quantity, amount_cents
        FROM clean.sales_lines
        WHERE (unit = 'kg' AND abs(quantity) > {MAX_KILOGRAMS})
           OR (unit = 'each' AND quantity <> round(quantity))
    """,
)

TILL_TOTAL = Check(
    number=11,
    key="till_total",
    family="two counts of one thing agree",
    name="The receipts add up to the till's own total",
    table="clean.sales_lines",
    lens="business",
    question="Accurate?",
    protects=MANAGERS + ". It is the only check that can say a whole day's money is right",
    prepare="Add the lines up per till per day. Join that to till_totals, "
    "the till's own count of the same money",
    limit="The two totals are equal, to the cent. A till with no lines, "
    "and lines with no till, both fail",
    catches=(1, 2, 3, 5, 6, 7, 9),
    sql="""
        SELECT t.store_id, t.till_id, t.business_date AS sales_date,
               t.net_total_cents AS till_cents, coalesce(l.lines_cents, 0) AS lines_cents,
               coalesce(l.lines_cents, 0) - t.net_total_cents AS difference_cents
        FROM clean.till_totals t
        LEFT JOIN (
            SELECT store_id, till_id, sales_date, sum(amount_cents) AS lines_cents
            FROM clean.sales_lines
            GROUP BY store_id, till_id, sales_date
        ) l
          ON l.store_id = t.store_id AND l.till_id = t.till_id AND l.sales_date = t.business_date
        WHERE coalesce(l.lines_cents, 0) <> t.net_total_cents
        UNION ALL
        SELECT l.store_id, l.till_id, l.sales_date, NULL AS till_cents,
               sum(l.amount_cents) AS lines_cents, sum(l.amount_cents) AS difference_cents
        FROM clean.sales_lines l
        LEFT JOIN clean.till_totals t
          ON t.store_id = l.store_id AND t.till_id = l.till_id AND t.business_date = l.sales_date
        WHERE t.store_id IS NULL
        GROUP BY l.store_id, l.till_id, l.sales_date
    """,
)

# ------------------------------------------------------------------ operational lens

# Every day the pipeline has loaded, for every store that was open that day.
EXPECTED_DAYS = f"""
    SELECT s.store_id, d.day
    FROM clean.stores s
    CROSS JOIN (
        SELECT DISTINCT business_date AS day FROM clean.till_totals
        UNION
        SELECT DISTINCT sales_date FROM clean.sales_lines WHERE sales_date <= {LAST_DAY}
    ) d
    WHERE NOT EXISTS (
        SELECT 1 FROM clean.calendar c
        WHERE c.store_id = s.store_id AND c.calendar_date = d.day AND c.kind = 'closed'
    )
"""

EVERY_STORE_REPORTED = Check(
    number=12,
    key="every_store_reported",
    family="nothing is missing",
    name="Every open store reported",
    table="clean.sales_lines",
    lens="operational",
    question="Complete?",
    protects=MANAGERS + ", and the replenishment team: a store with no sales orders nothing",
    prepare="Make the list of days a store should have: every loaded day, for every store, "
    "less the days the calendar says it was closed",
    limit="Each open store has at least one sales line on each day",
    catches=(7, 8),
    sql=f"""
        SELECT e.store_id, e.day AS sales_date, 'no sales lines for an open store' AS problem
        FROM ({EXPECTED_DAYS}) e
        LEFT JOIN (SELECT DISTINCT store_id, sales_date FROM clean.sales_lines) l
          ON l.store_id = e.store_id AND l.sales_date = e.day
        WHERE l.store_id IS NULL
    """,
)

# One row per store per day: its lines, and its normal day. The normal day is the
# average of the same weekday in the four weeks before. Days in the calendar are
# left out on both sides: a holiday is not a normal day, and is not judged as one.
NORMAL_DAY = f"""
    SELECT today.store_id, today.sales_date, today.lines,
           count(before.lines) AS days_compared, avg(before.lines) AS normal_lines
    FROM (
        SELECT store_id, sales_date, count(*) AS lines FROM clean.sales_lines
        WHERE sales_date <= {LAST_DAY}
        GROUP BY store_id, sales_date
    ) today
    JOIN (
        SELECT store_id, sales_date, count(*) AS lines FROM clean.sales_lines
        GROUP BY store_id, sales_date
    ) before
      ON before.store_id = today.store_id
     AND before.sales_date IN (today.sales_date - 7, today.sales_date - 14,
                               today.sales_date - 21, today.sales_date - 28)
    WHERE NOT EXISTS (SELECT 1 FROM clean.calendar c WHERE c.store_id = today.store_id
                      AND c.calendar_date IN (today.sales_date, before.sales_date))
    GROUP BY today.store_id, today.sales_date, today.lines
"""

NORMAL_VOLUME = Check(
    number=13,
    key="normal_volume",
    family="it looks like a normal day",
    name="A store's day is near its normal day",
    table="clean.sales_lines",
    lens="operational",
    question="Complete?",
    protects=MANAGERS,
    prepare="Count the lines per store per day. Work out a normal day: the average of "
    "the same weekday in the four weeks before. Leave out the days in the calendar",
    limit=f"Between {VOLUME_LOW:.0%} and {VOLUME_HIGH:.0%} of the normal day, "
    "when at least three earlier days can be compared",
    catches=(9,),
    sql=f"""
        SELECT store_id, sales_date, lines, CAST(round(normal_lines) AS INTEGER) AS normal_lines,
               CAST(round(100.0 * lines / normal_lines) AS INTEGER) AS percent_of_normal
        FROM ({NORMAL_DAY}) n
        WHERE days_compared >= 3
          AND (lines < {VOLUME_LOW} * normal_lines OR lines > {VOLUME_HIGH} * normal_lines)
    """,
)

SALES_CHECKS = [
    REQUIRED_CELLS, TRUE_KEY_UNIQUE, RIGHT_SHAPE, LINE_ADDS_UP, EVERY_SALE_HAS_A_PRODUCT,
    REFUND_GIVES_MONEY_BACK, REFUND_NOT_LARGER_THAN_SALE, PRICE_OF_THE_DAY,
    INSIDE_OPENING_HOURS, BELIEVABLE_QUANTITY, TILL_TOTAL,
    EVERY_STORE_REPORTED, NORMAL_VOLUME,
]  # fmt: skip

BY_KEY = {check.key: check for check in SALES_CHECKS}
