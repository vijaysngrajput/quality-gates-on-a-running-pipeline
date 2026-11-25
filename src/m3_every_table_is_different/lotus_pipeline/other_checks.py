"""The checks for the five other tables. Every table is different.

    products          typed by the buying team. A price has a start and an end
    stores            six rows, typed once. Wrong here is wrong everywhere
    stock_counts      a snapshot: one count per product per store per day
    supplier_prices   a file Lotus does not control
    members           people. Typed by customers in an app

The checks for sales_lines would be useless here. A price list has no till to be compared
with, and a list of people has no "normal day". So each table gets its own check card,
and from the card its own checks.

A check returns one row per bad thing. For these tables a row says `thing` (which
product, store, file or member) and `day`.
"""

from lotus_pipeline.checks import Check
from lotus_pipeline.load import TABLES
from lotus_pipeline.sales_checks import EXPECTED_DAYS, LAST_DAY

FOREVER = "DATE '9999-12-31'"
EVERYONE = "Everyone who reads the two outputs: a sales line is joined to this table"
REPLENISHMENT = "The replenishment team: they order from this number"
FINANCE = "Finance: they pay suppliers from this price"
MARKETING = "Marketing, and the member who gets the email"

EXPECTED_STORES = 6  # Lotus has six stores. Head office says so, not the data
MAX_PRICE_CHANGE = 2  # a new price is at most twice, and at least half, the old one
MAX_COST_CHANGE = 2  # the same for a supplier's cost against its last file

# ================================================================== products

ONE_PRICE_AT_A_TIME = Check(
    number=17,
    key="one_price_at_a_time",
    family="a row agrees with its neighbours",
    name="A product has one price on any day",
    table="clean.products",
    lens="data",
    question="Unique?",
    protects=EVERYONE + ". Two prices on one day join one sales line twice",
    prepare="Join the table to itself: every price row of a product against its later rows",
    limit="No two price rows of one product cover the same day",
    sql=f"""
        SELECT a.product_id AS thing, b.valid_from AS day, a.product_name,
               a.valid_from AS first_from, a.valid_to AS first_to, b.valid_from AS second_from
        FROM clean.products a
        JOIN clean.products b
          ON b.product_id = a.product_id AND b.valid_from > a.valid_from
         AND b.valid_from <= coalesce(a.valid_to, {FOREVER})
    """,
)

NO_GAP_BETWEEN_PRICES = Check(
    number=18,
    key="no_gap_between_prices",
    family="a row agrees with its neighbours",
    name="A product on sale has a price on every day",
    table="clean.products",
    lens="data",
    question="Complete?",
    protects=EVERYONE + ". A sale on a day with no price is dropped by the join",
    prepare="For every price that ends, look for the price that starts the next day",
    limit="An active price that ends is followed, the next day, by another price",
    sql="""
        SELECT a.product_id AS thing, a.valid_to + 1 AS day, a.product_name,
               a.valid_to AS price_ends
        FROM clean.products a
        LEFT JOIN clean.products b
          ON b.product_id = a.product_id AND b.valid_from = a.valid_to + 1
        WHERE a.valid_to IS NOT NULL AND a.status = 'active' AND b.product_id IS NULL
    """,
)

PRODUCT_VALUES_ALLOWED = Check(
    number=19,
    key="product_values_allowed",
    family="values are allowed",
    name="A product row makes sense",
    table="clean.products",
    lens="data",
    question="Valid?",
    protects=EVERYONE,
    prepare="Nothing",
    limit="A name. A price above zero. A unit of each or kg. A status of active or retired. "
    "A start that is not after its end",
    sql="""
        SELECT product_id AS thing, valid_from AS day, product_name, unit, unit_price_cents,
               status, valid_to
        FROM clean.products
        WHERE product_name IS NULL OR unit_price_cents IS NULL OR unit_price_cents <= 0
           OR unit IS NULL OR unit NOT IN ('each', 'kg')
           OR status IS NULL OR status NOT IN ('active', 'retired')
           OR valid_from IS NULL OR valid_from > valid_to
    """,
)

PRICE_CHANGE_IS_BELIEVABLE = Check(
    number=20,
    key="price_change_is_believable",
    family="it did not change by surprise",
    name="A new price is near the old price",
    table="clean.products",
    lens="business",
    question="Accurate?",
    protects="Customers and store managers: a typing mistake in a price is charged at every till",
    prepare="Join each price row to the row that ended the day before",
    limit=f"A new price is at most {MAX_PRICE_CHANGE} times the old one, and at least half of it",
    sql=f"""
        SELECT new.product_id AS thing, new.valid_from AS day, new.product_name,
               old.unit_price_cents AS old_price_cents, new.unit_price_cents AS new_price_cents
        FROM clean.products new
        JOIN clean.products old
          ON old.product_id = new.product_id AND old.valid_to + 1 = new.valid_from
        WHERE new.unit_price_cents > {MAX_PRICE_CHANGE} * old.unit_price_cents
           OR new.unit_price_cents * {MAX_PRICE_CHANGE} < old.unit_price_cents
    """,
)

RETIRED_MEANS_ENDED = Check(
    number=21,
    key="retired_means_ended",
    family="a row agrees with itself",
    name="A retired product has a last day",
    table="clean.products",
    lens="business",
    question="Consistent?",
    protects=REPLENISHMENT + ". A retired product with no last day is ordered for ever",
    prepare="Nothing. Both facts are on one row",
    limit="status is retired exactly when the last price row has an end date and nothing follows",
    sql="""
        SELECT product_id AS thing, valid_from AS day, product_name, status, valid_to
        FROM clean.products
        WHERE status = 'retired' AND valid_to IS NULL
    """,
)

PRODUCT_CHECKS = [
    ONE_PRICE_AT_A_TIME, NO_GAP_BETWEEN_PRICES, PRODUCT_VALUES_ALLOWED,
    PRICE_CHANGE_IS_BELIEVABLE, RETIRED_MEANS_ENDED,
]  # fmt: skip

# ================================================================== stores

SIX_STORES_ONCE = Check(
    number=22,
    key="six_stores_once",
    family="the key is unique",
    name="There are six stores, each once",
    table="clean.stores",
    lens="data",
    question="Unique?",
    protects=EVERYONE + ". A store that is there twice doubles its sales",
    prepare="Count the rows. For a table this small, the exact count is a fact you can know",
    limit=f"Exactly {EXPECTED_STORES} rows, and no store_id twice",
    sql=f"""
        SELECT 'the stores table' AS thing, {LAST_DAY} AS day,
               count(*) AS stores_found, {EXPECTED_STORES} AS stores_expected
        FROM clean.stores
        HAVING count(*) <> {EXPECTED_STORES}
        UNION ALL
        SELECT store_id AS thing, {LAST_DAY} AS day, count(*) AS stores_found, 1 AS stores_expected
        FROM clean.stores
        GROUP BY store_id
        HAVING count(*) > 1
    """,
)

STORE_VALUES_ALLOWED = Check(
    number=23,
    key="store_values_allowed",
    family="values are allowed",
    name="A store row makes sense",
    table="clean.stores",
    lens="data",
    question="Valid?",
    protects=EVERYONE + ", and the opening-hours check, which reads these hours",
    prepare="Nothing",
    limit="A name, a town and a manager. It opens before it closes. It has at least one till",
    sql=f"""
        SELECT store_id AS thing, {LAST_DAY} AS day, store_name, opens_at, closes_at, tills
        FROM clean.stores
        WHERE store_name IS NULL OR town IS NULL OR manager_name IS NULL
           OR opens_at IS NULL OR closes_at IS NULL OR opens_at >= closes_at
           OR tills IS NULL OR tills < 1
    """,
)

EVERY_STORE_IS_KNOWN = Check(
    number=24,
    key="every_store_is_known",
    family="it points at a row that exists",
    name="Every file is from a store we know",
    table="clean.stores",
    lens="data",
    question="Consistent?",
    protects=EVERYONE + ". Sales of a store that is not in this table are dropped by the join",
    prepare="Collect the store_id of every sales line, till total and stock count, "
    "and join them to stores",
    limit="Every store_id in the three store files is in the stores table",
    sql=f"""
        SELECT used.store_id AS thing, {LAST_DAY} AS day, used.found_in
        FROM (
            SELECT DISTINCT store_id, 'sales_lines' AS found_in FROM raw.sales_lines
            UNION
            SELECT DISTINCT store_id, 'till_totals' FROM raw.till_totals
            UNION
            SELECT DISTINCT store_id, 'stock_counts' FROM raw.stock_counts
        ) used
        LEFT JOIN clean.stores s ON s.store_id = used.store_id
        WHERE s.store_id IS NULL
    """,
)

_STORE_COLUMNS = ", ".join(TABLES["stores"])

STORES_DID_NOT_CHANGE = Check(
    number=25,
    key="stores_did_not_change",
    family="it did not change by surprise",
    name="The stores table is the same as yesterday",
    table="raw.stores",
    lens="operational",
    question="Consistent?",
    protects=EVERYONE + ". This table is typed once and rarely changed, so any change is news",
    prepare="Take the newest file and the file before it, and compare them row by row",
    limit="No row is new, gone or different. A real change is confirmed by a person",
    sql=f"""
        SELECT store_id AS thing, {LAST_DAY} AS day, store_name, opens_at, closes_at,
               manager_name, tills
        FROM (
            SELECT {_STORE_COLUMNS} FROM raw.stores
            WHERE _file_name = (SELECT max(_file_name) FROM raw.stores)
            EXCEPT
            SELECT {_STORE_COLUMNS} FROM raw.stores
            WHERE _file_name = (
                SELECT max(_file_name) FROM raw.stores
                WHERE _file_name < (SELECT max(_file_name) FROM raw.stores)
            )
        ) changed
    """,
)

STORE_CHECKS = [SIX_STORES_ONCE, STORE_VALUES_ALLOWED, EVERY_STORE_IS_KNOWN, STORES_DID_NOT_CHANGE]

# ================================================================== stock_counts

A_COUNT_FOR_EVERY_DAY = Check(
    number=26,
    key="a_count_for_every_day",
    family="nothing is missing",
    name="A snapshot has a row for every day",
    table="clean.stock_counts",
    lens="data",
    question="Complete?",
    protects=REPLENISHMENT + ". A missing count is read as zero, and the store over-orders",
    prepare="Make the list of counts a day should have: every open store, "
    "every product on sale that day. Then look for the ones that are not there",
    limit="Every product on sale has a count, in every open store, on every day",
    catches=(12,),
    sql=f"""
        SELECT e.store_id, e.day AS sales_date, p.product_id, p.product_name,
               'no count for this product on this day' AS problem
        FROM ({EXPECTED_DAYS}) e
        JOIN clean.products p
          ON e.day BETWEEN p.valid_from AND coalesce(p.valid_to, {FOREVER})
        LEFT JOIN clean.stock_counts c
          ON c.store_id = e.store_id AND c.product_id = p.product_id AND c.count_date = e.day
        WHERE c.product_id IS NULL
    """,
)

ONE_COUNT_PER_DAY = Check(
    number=27,
    key="one_count_per_day",
    family="the key is unique",
    name="A product is counted once a day",
    table="clean.stock_counts",
    lens="data",
    question="Unique?",
    protects=REPLENISHMENT + ". Two counts for one shelf: which one is true?",
    prepare="Group by the key: day, store and product",
    limit="Each count_date + store_id + product_id is there exactly once",
    sql="""
        SELECT store_id, count_date AS sales_date, product_id, count(*) AS times
        FROM clean.stock_counts
        GROUP BY store_id, count_date, product_id
        HAVING count(*) > 1
    """,
)

COUNT_VALUES_ALLOWED = Check(
    number=28,
    key="count_values_allowed",
    family="values are allowed",
    name="A count is a believable count",
    table="clean.stock_counts",
    lens="data",
    question="Valid?",
    protects=REPLENISHMENT,
    prepare="Join each count to its product for the unit: pieces or kilograms",
    limit="Not empty, not below zero. Pieces are whole numbers. Counted on its own day, "
    "by scanner or by hand",
    sql=f"""
        SELECT c.store_id, c.count_date AS sales_date, c.product_id, c.counted_qty,
               c.counted_by, c.counted_at
        FROM clean.stock_counts c
        LEFT JOIN clean.products p
          ON p.product_id = c.product_id
         AND c.count_date BETWEEN p.valid_from AND coalesce(p.valid_to, {FOREVER})
        WHERE c.counted_qty IS NULL OR c.counted_qty < 0
           OR (p.unit = 'each' AND c.counted_qty <> round(c.counted_qty))
           OR c.counted_by IS NULL OR c.counted_by NOT IN ('scanner', 'hand')
           OR c.counted_at IS NULL OR CAST(c.counted_at AS DATE) <> c.count_date
    """,
)

STOCK_CHECKS = [A_COUNT_FOR_EVERY_DAY, ONE_COUNT_PER_DAY, COUNT_VALUES_ALLOWED]

# ================================================================== supplier_prices

_EXPECTED_COLUMNS = " UNION ALL ".join(
    f"SELECT '{table}' AS table_name, '{column}' AS column_name"
    for table, columns in TABLES.items()
    for column in columns
)

FILES_HAVE_THEIR_COLUMNS = Check(
    number=29,
    key="files_have_their_columns",
    family="it has the columns we expect",
    name="A file has the columns we expect",
    table="raw.file_columns",
    lens="data",
    question="Valid?",
    protects=FINANCE + ". A renamed column loads as an empty column, and nothing fails",
    prepare="Keep the first line of every file. Put the columns we expect beside "
    "the columns each file really had",
    limit="No expected column is missing. A column we did not expect is reported too",
    catches=(10,),
    sql=f"""
        SELECT f.file_name AS thing, f.table_name, e.column_name,
               'an expected column is missing' AS problem, 'breaking' AS kind_of_change
        FROM (SELECT DISTINCT file_name, table_name FROM raw.file_columns) f
        JOIN ({_EXPECTED_COLUMNS}) e ON e.table_name = f.table_name
        LEFT JOIN raw.file_columns found
          ON found.file_name = f.file_name AND found.column_name = e.column_name
        WHERE found.column_name IS NULL
        UNION ALL
        SELECT found.file_name AS thing, found.table_name, found.column_name,
               'a column we did not expect' AS problem, 'additive' AS kind_of_change
        FROM raw.file_columns found
        LEFT JOIN ({_EXPECTED_COLUMNS}) e
          ON e.table_name = found.table_name AND e.column_name = found.column_name
        WHERE e.column_name IS NULL
    """,
)

COST_IS_FILLED = Check(
    number=30,
    key="cost_is_filled",
    family="cells are filled",
    name="Every supplier row has a cost",
    table="clean.supplier_prices",
    lens="data",
    question="Complete?",
    protects=FINANCE + ". An order with no cost cannot be approved or paid",
    prepare="Nothing",
    limit="No empty unit_cost, product, supplier, unit or date",
    catches=(10,),
    sql="""
        SELECT supplier_id || ' ' || product_id AS thing, price_date AS day,
               supplier_id, product_id, unit_cost_cents
        FROM clean.supplier_prices
        WHERE unit_cost_cents IS NULL OR product_id IS NULL OR supplier_id IS NULL
           OR unit IS NULL OR price_date IS NULL
    """,
)

COST_NEAR_THE_LAST_FILE = Check(
    number=31,
    key="cost_near_the_last_file",
    family="it did not change by surprise",
    name="A cost is near the cost in the last file",
    table="clean.supplier_prices",
    lens="business",
    question="Accurate?",
    protects=FINANCE + ". A cost that is 100 times too high is still a valid number",
    prepare="Join each row to the same supplier and product in the file before",
    limit=f"A cost is at most {MAX_COST_CHANGE} times the last one, and at least half of it",
    catches=(11,),
    sql=f"""
        SELECT new.supplier_id || ' ' || new.product_id AS thing, new.price_date AS day,
               new.supplier_id, new.product_id,
               old.unit_cost_cents AS last_cost_cents, new.unit_cost_cents AS new_cost_cents
        FROM clean.supplier_prices new
        JOIN clean.supplier_prices old
          ON old.supplier_id = new.supplier_id AND old.product_id = new.product_id
         AND old.price_date = (
             SELECT max(before.price_date) FROM clean.supplier_prices before
             WHERE before.supplier_id = new.supplier_id AND before.product_id = new.product_id
               AND before.price_date < new.price_date AND before.unit_cost_cents IS NOT NULL
         )
        WHERE new.unit_cost_cents > {MAX_COST_CHANGE} * old.unit_cost_cents
           OR new.unit_cost_cents * {MAX_COST_CHANGE} < old.unit_cost_cents
    """,
)

COST_BELOW_THE_SHELF_PRICE = Check(
    number=32,
    key="cost_below_the_shelf_price",
    family="two tables agree",
    name="A product costs less than it sells for",
    table="clean.supplier_prices",
    lens="business",
    question="Accurate?",
    protects=FINANCE,
    prepare="Join each cost to the product's shelf price valid on that day",
    limit="unit_cost_cents is below unit_price_cents",
    catches=(11,),
    sql=f"""
        SELECT s.supplier_id || ' ' || s.product_id AS thing, s.price_date AS day,
               s.supplier_id, s.product_id, p.product_name,
               s.unit_cost_cents, p.unit_price_cents AS shelf_price_cents
        FROM clean.supplier_prices s
        JOIN clean.products p
          ON p.product_id = s.product_id
         AND s.price_date BETWEEN p.valid_from AND coalesce(p.valid_to, {FOREVER})
        WHERE s.unit_cost_cents >= p.unit_price_cents
    """,
)

SUPPLIER_ROW_FITS_THE_PRODUCT = Check(
    number=33,
    key="supplier_row_fits_the_product",
    family="two tables agree",
    name="A supplier row fits its product",
    table="clean.supplier_prices",
    lens="data",
    question="Consistent?",
    protects=FINANCE + ". A cost per kilogram read as a cost per piece is a wrong payment",
    prepare="Group by the key. Join each row to the product for its unit",
    limit="Each supplier + product + date is there once. The product exists. The unit is "
    "the product's unit",
    sql=f"""
        SELECT s.supplier_id || ' ' || s.product_id AS thing, s.price_date AS day,
               s.supplier_id, s.product_id, s.unit AS supplier_unit, p.unit AS product_unit,
               'the product is unknown, or its unit differs' AS problem
        FROM clean.supplier_prices s
        LEFT JOIN clean.products p
          ON p.product_id = s.product_id
         AND s.price_date BETWEEN p.valid_from AND coalesce(p.valid_to, {FOREVER})
        WHERE p.product_id IS NULL OR p.unit <> s.unit
        UNION ALL
        SELECT supplier_id || ' ' || product_id AS thing, price_date AS day,
               supplier_id, product_id, NULL AS supplier_unit, NULL AS product_unit,
               'the same row is there more than once' AS problem
        FROM clean.supplier_prices
        GROUP BY supplier_id, product_id, price_date
        HAVING count(*) > 1
    """,
)

SUPPLIER_CHECKS = [
    FILES_HAVE_THEIR_COLUMNS, COST_IS_FILLED, COST_NEAR_THE_LAST_FILE,
    COST_BELOW_THE_SHELF_PRICE, SUPPLIER_ROW_FITS_THE_PRODUCT,
]  # fmt: skip

# ================================================================== members
# These checks never return a name, an email or a phone number. A check's output is
# printed, logged and copied into other tables. Personal data must not travel with it.

ONE_PERSON_ONCE = Check(
    number=34,
    key="one_person_once",
    family="the key is unique",
    name="One person is one member",
    table="clean.members",
    lens="data",
    question="Unique?",
    protects=MARKETING + ": two emails, and two sets of points",
    prepare="Clean the email first: remove spaces around it, and make it lower case. "
    "Then group by the cleaned email",
    limit="Each cleaned email belongs to one member_id",
    catches=(13,),
    sql="""
        SELECT min(member_id) AS thing, max(joined_on) AS day,
               min(member_id) AS first_member_id, max(member_id) AS second_member_id,
               count(*) AS members_with_this_email
        FROM clean.members
        GROUP BY lower(trim(email))
        HAVING count(*) > 1
    """,
)

EMAIL_HAS_THE_RIGHT_SHAPE = Check(
    number=35,
    key="email_has_the_right_shape",
    family="values are allowed",
    name="An email has the shape of an email",
    table="clean.members",
    lens="data",
    question="Valid?",
    protects=MARKETING + ". An email that cannot be delivered, or reaches a stranger",
    prepare="Take the email apart at the @ sign",
    limit="One @, with text before it. A dot after it. No space inside",
    sql="""
        SELECT member_id AS thing, joined_on AS day, 'the email has the wrong shape' AS problem
        FROM clean.members
        WHERE email IS NULL
           OR length(email) - length(replace(email, '@', '')) <> 1
           OR strpos(email, '@') < 2
           OR strpos(substr(email, strpos(email, '@') + 2), '.') = 0
           OR strpos(trim(email), ' ') > 0
           OR substr(email, length(email), 1) = '.'
    """,
)

MEMBER_CELLS_FILLED = Check(
    number=36,
    key="member_cells_filled",
    family="cells are filled",
    name="A member row has what marketing needs",
    table="clean.members",
    lens="data",
    question="Complete?",
    protects=MARKETING + ". An email to someone who did not say yes is against the rules",
    prepare="Decide which cells may be empty: a phone number is optional in the app",
    limit="A name, a home store, a join date, and a yes or no to marketing. A phone may be empty",
    sql="""
        SELECT member_id AS thing, joined_on AS day,
               'a cell that must be filled is empty' AS problem
        FROM clean.members
        WHERE member_id IS NULL OR full_name IS NULL OR trim(full_name) = ''
           OR home_store_id IS NULL OR joined_on IS NULL OR marketing_opt_in IS NULL
    """,
)

MEMBER_FITS_THE_REAL_WORLD = Check(
    number=37,
    key="member_fits_the_real_world",
    family="it points at a row that exists",
    name="A member has a real store and a past join date",
    table="clean.members",
    lens="business",
    question="Consistent?",
    protects=MARKETING + ". Offers are sent by home store",
    prepare="Join each member to stores. Find the newest day the pipeline has loaded",
    limit="home_store_id is in the stores table. joined_on is not after the newest loaded day",
    sql=f"""
        SELECT m.member_id AS thing, m.joined_on AS day, m.home_store_id,
               'an unknown home store, or a join date in the future' AS problem
        FROM clean.members m
        LEFT JOIN clean.stores s ON s.store_id = m.home_store_id
        WHERE s.store_id IS NULL OR m.joined_on > {LAST_DAY}
    """,
)

MEMBER_CHECKS = [
    ONE_PERSON_ONCE, EMAIL_HAS_THE_RIGHT_SHAPE, MEMBER_CELLS_FILLED, MEMBER_FITS_THE_REAL_WORLD,
]  # fmt: skip

PERSONAL_COLUMNS = ("full_name", "email", "phone")

OTHER_CHECKS = PRODUCT_CHECKS + STORE_CHECKS + STOCK_CHECKS + SUPPLIER_CHECKS + MEMBER_CHECKS
