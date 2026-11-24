"""The fixed facts of Lotus Retails: its calendar, its stores, its products and its suppliers."""

from datetime import date, time, timedelta

FIRST_DAY = date(2026, 7, 27)  # a Monday
DAYS = 35  # five full weeks: 2026-07-27 to 2026-08-30
LAST_DAY = FIRST_DAY + timedelta(days=DAYS - 1)

# Weeks 1 to 4 are clean. Every planted case is in week 5, the bonus week.
BONUS_WEEK_START = date(2026, 8, 24)  # a Monday
BONUS_WEEK_END = date(2026, 8, 30)  # a Sunday
SIGN_OFF_DAY = date(2026, 8, 31)  # the Monday the bonus is signed off

RUN_HOUR = 2  # the nightly pipeline runs at 02:00, for the business day that just ended
BONUS_RATE_PERCENT = 1  # a manager's bonus is 1% of the store's net sales for the week

# Things that look wrong and are not.
HOLIDAY = date(2026, 8, 26)  # Founders' Day, an invented public holiday. Low sales, short hours
HOLIDAY_NAME = "Founders' Day"
HOLIDAY_HOURS = (time(10, 0), time(16, 0))
CLOSED = {("S01", date(2026, 8, 27))}  # a planned refit. No files, and that is correct

# store_id -> name, town, opens, closes, manager, size (1.0 is an average store)
STORES = {
    "S01": ("Lotus Ashby High Street", "Ashby", time(8, 0), time(22, 0), "Marta Lindqvist", 1.25),
    "S02": ("Lotus Ashby Market Square", "Ashby", time(8, 0), time(22, 0), "Dana Okafor", 1.10),
    "S03": ("Lotus Calder Bridge", "Calder", time(8, 0), time(21, 0), "Tomas Reyes", 1.00),
    "S04": ("Lotus Calder Station", "Calder", time(7, 0), time(22, 0), "Ines Marchetti", 1.00),
    "S05": ("Lotus Fenwick Green", "Fenwick", time(8, 0), time(20, 0), "Samir Haddad", 0.75),
    "S06": ("Lotus Fenwick Harbour", "Fenwick", time(8, 0), time(21, 0), "Aiko Tanaka", 0.90),
}
TILLS = ("T1", "T2")

# How busy each day of the week is, Monday first.
WEEKDAY_FACTOR = (0.90, 0.90, 0.95, 1.00, 1.15, 1.30, 1.05)
HOLIDAY_FACTOR = 0.40

# product_id -> name, category, unit, price in cents, supplier
PRODUCTS = {
    "P-001": ("Whole milk 2L", "Dairy", "each", 289, "SUP-A"),
    "P-002": ("Oat milk 1L", "Dairy", "each", 349, "SUP-A"),
    "P-003": ("Butter 250g", "Dairy", "each", 349, "SUP-A"),
    "P-004": ("Cheddar 200g", "Dairy", "each", 449, "SUP-A"),
    "P-005": ("Greek yogurt 500g", "Dairy", "each", 399, "SUP-A"),
    "P-006": ("Eggs, 12", "Dairy", "each", 489, "SUP-A"),
    "P-007": ("Mozzarella", "Dairy", "each", 379, "SUP-A"),
    "P-008": ("Sourdough loaf", "Bakery", "each", 525, "SUP-A"),
    "P-009": ("White bread", "Bakery", "each", 249, "SUP-A"),
    "P-010": ("Croissants, 4", "Bakery", "each", 399, "SUP-A"),
    "P-011": ("Bagels, 6", "Bakery", "each", 359, "SUP-A"),
    "P-012": ("Bananas", "Fruit and vegetables", "kg", 129, "SUP-B"),
    "P-013": ("Tomatoes", "Fruit and vegetables", "kg", 399, "SUP-B"),
    "P-014": ("Apples", "Fruit and vegetables", "kg", 349, "SUP-B"),
    "P-015": ("Potatoes", "Fruit and vegetables", "kg", 149, "SUP-B"),
    "P-016": ("Onions", "Fruit and vegetables", "kg", 159, "SUP-B"),
    "P-017": ("Carrots", "Fruit and vegetables", "kg", 139, "SUP-B"),
    "P-018": ("Avocado", "Fruit and vegetables", "each", 179, "SUP-B"),
    "P-019": ("Baby spinach", "Fruit and vegetables", "each", 329, "SUP-B"),
    "P-020": ("Strawberries 400g", "Fruit and vegetables", "each", 459, "SUP-B"),
    "P-021": ("Chicken breast 500g", "Meat and fish", "each", 799, "SUP-B"),
    "P-022": ("Salmon fillet", "Meat and fish", "each", 1195, "SUP-B"),
    "P-023": ("Minced beef 500g", "Meat and fish", "each", 649, "SUP-B"),
    "P-024": ("Olive oil 500ml", "Pantry", "each", 949, "SUP-C"),
    "P-025": ("Pasta 500g", "Pantry", "each", 189, "SUP-C"),
    "P-026": ("Tomato sauce", "Pantry", "each", 259, "SUP-C"),
    "P-027": ("Basmati rice 1kg", "Pantry", "each", 429, "SUP-C"),
    "P-028": ("Coffee beans 250g", "Pantry", "each", 899, "SUP-C"),
    "P-029": ("Granola 500g", "Pantry", "each", 549, "SUP-C"),
    "P-030": ("Dark chocolate 100g", "Pantry", "each", 279, "SUP-C"),
    "P-031": ("Flour 1kg", "Pantry", "each", 199, "SUP-C"),
    "P-032": ("Still water 500ml", "Drinks", "each", 59, "SUP-C"),
    "P-033": ("Sparkling water 1L", "Drinks", "each", 129, "SUP-C"),
    "P-034": ("Orange juice 1L", "Drinks", "each", 379, "SUP-C"),
    "P-035": ("Cola 1.5L", "Drinks", "each", 229, "SUP-C"),
    "P-036": ("Ice cream 500ml", "Frozen", "each", 599, "SUP-A"),
    "P-037": ("Frozen peas 1kg", "Frozen", "each", 289, "SUP-A"),
    "P-038": ("Paper towels", "Household", "each", 399, "SUP-C"),
    "P-039": ("Dish soap", "Household", "each", 329, "SUP-C"),
    "P-040": ("Laundry liquid", "Household", "each", 899, "SUP-C"),
}

# A price that changes: product_id -> (first day of the new price, new price in cents).
# The buying team types the new row the day before it starts.
PRICE_CHANGES = {
    "P-006": (date(2026, 8, 10), 519),  # Eggs. Every store charges the new price. A clean change
    "P-003": (date(2026, 8, 28), 399),  # Butter. One store keeps the old price: planted case 4
}

# A product that is retired: product_id -> its last day on sale.
RETIRED = {"P-011": date(2026, 8, 16)}  # Bagels

# A product the tills already sell, and the buying team has not typed yet: planted case 6.
UNLISTED_PRODUCT = ("P-041", "Pumpkin soup 500ml", 599)

# supplier_id -> the day of the week it sends its price file (Monday is 0).
SUPPLIERS = {"SUP-A": 0, "SUP-B": 1, "SUP-C": 4}


def all_days() -> list[date]:
    return [FIRST_DAY + timedelta(days=n) for n in range(DAYS)]


def price_on(product_id: str, day: date) -> int:
    """The shelf price of a product on a day, in cents."""
    if product_id == UNLISTED_PRODUCT[0]:
        return UNLISTED_PRODUCT[2]
    change = PRICE_CHANGES.get(product_id)
    if change and day >= change[0]:
        return change[1]
    return PRODUCTS[product_id][3]


def on_sale(product_id: str, day: date) -> bool:
    last = RETIRED.get(product_id)
    return last is None or day <= last


def opening_hours(store_id: str, day: date) -> tuple[time, time]:
    if day == HOLIDAY:
        return HOLIDAY_HOURS
    return STORES[store_id][2], STORES[store_id][3]


def is_open(store_id: str, day: date) -> bool:
    return (store_id, day) not in CLOSED


def week_start(day: date) -> date:
    """The Monday of the week a day is in."""
    return day - timedelta(days=day.weekday())
