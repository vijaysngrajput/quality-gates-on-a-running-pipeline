"""The warehouse: where the tables live.

The same SQL runs on two databases:

    postgres  the real warehouse, in Docker
    duckdb    one file on your disk, for the two-minute run and the tests

Every other file talks to this small class, and never to a database directly.
In SQL, write `?` where a value goes.
"""

from datetime import date, datetime
from decimal import Decimal

from lotus_pipeline import config

SCHEMAS = ("raw", "clean", "output", "ops")

HOW_TO_START = (
    "Cannot reach the warehouse (Postgres on {host}:{port}).\n"
    "Start it from the project folder:\n"
    "  docker compose up -d warehouse\n"
    "Or use a database in one file, with nothing to start:\n"
    "  macOS, Linux:  export LOTUS_WAREHOUSE=duckdb\n"
    '  Windows:       $env:LOTUS_WAREHOUSE="duckdb"'
)


class WarehouseDown(ConnectionError):
    pass


class Warehouse:
    def __init__(self, kind: str, con):
        self.kind = kind
        self.con = con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        if self.kind == "postgres":
            # With no values to fill in, send the SQL as it is: a % in it stays a %.
            self.con.execute(sql.replace("?", "%s"), params) if params else self.con.execute(sql)
        else:
            self.con.execute(sql, list(params))

    def query(self, sql: str, params: tuple | list = ()) -> list[dict]:
        """Run a SELECT. Returns one dictionary per row."""
        if self.kind == "postgres":
            cur = (
                self.con.execute(sql.replace("?", "%s"), params)
                if params
                else self.con.execute(sql)
            )
        else:
            cur = self.con.execute(sql, list(params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def value(self, sql: str, params: tuple | list = ()):
        """The first column of the first row."""
        rows = self.query(sql, params)
        return next(iter(rows[0].values())) if rows else None

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        names = ", ".join(columns)
        if self.kind == "postgres":
            with self.con.cursor() as cur:
                with cur.copy(f"COPY {table} ({names}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)
        else:
            import pandas as pd

            # DuckDB can read a pandas table by name. That is much faster than row by row.
            frame = pd.DataFrame(rows, columns=columns, dtype=object)
            self.con.register("incoming_rows", frame)
            self.con.execute(f"INSERT INTO {table} ({names}) SELECT * FROM incoming_rows")
            self.con.unregister("incoming_rows")

    def commit(self) -> None:
        self.con.commit()

    def rollback(self) -> None:
        self.con.rollback()

    def close(self) -> None:
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, error_type, error, traceback):
        if error_type is None:
            self.commit()
        else:
            try:
                self.rollback()
            except Exception:  # nothing was open to roll back: the step had already committed
                pass
        self.close()


def _connect_postgres() -> Warehouse:
    import psycopg

    where = dict(
        host=config.PG_HOST, port=config.PG_PORT, user=config.PG_USER,
        password=config.PG_PASSWORD, connect_timeout=5,
    )  # fmt: skip
    try:
        # Each milestone has its own database. Make it the first time.
        with psycopg.connect(dbname="lotus", autocommit=True, **where) as admin:
            found = admin.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (config.DATABASE,)
            ).fetchone()
            if not found:
                admin.execute(f'CREATE DATABASE "{config.DATABASE}"')
        return Warehouse("postgres", psycopg.connect(dbname=config.DATABASE, **where))
    except psycopg.OperationalError as error:
        raise WarehouseDown(
            HOW_TO_START.format(host=config.PG_HOST, port=config.PG_PORT)
        ) from error


def _connect_duckdb() -> Warehouse:
    import duckdb

    config.DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.DUCKDB_PATH))
    con.begin()
    return Warehouse("duckdb", con)


def connect() -> Warehouse:
    """Open the warehouse, and make sure its four schemas exist."""
    if config.WAREHOUSE == "duckdb":
        warehouse = _connect_duckdb()
    elif config.WAREHOUSE == "postgres":
        warehouse = _connect_postgres()
    else:
        raise ValueError(f"LOTUS_WAREHOUSE must be postgres or duckdb, not {config.WAREHOUSE!r}")
    for schema in SCHEMAS:
        warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return warehouse


def plain(value):
    """A value from the warehouse as plain Python: Decimal and dates become text or numbers."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    return value
