"""
Drop and recreate all CMS HealthFlow PostgreSQL schemas and tables.

USE WITH CAUTION — this deletes all data.

Run: python scripts/reset_db.py [--confirm]
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

sys.path.insert(0, ".")
from api.db import engine


def reset(dry_run: bool = True) -> None:
    statements = [
        "DROP SCHEMA IF EXISTS gold CASCADE",
        "DROP SCHEMA IF EXISTS silver CASCADE",
        "DROP SCHEMA IF EXISTS bronze CASCADE",
        "DROP TABLE IF EXISTS public.pipeline_runs CASCADE",
    ]

    sql_files = ["sql/01_init_schemas.sql", "sql/02_create_tables.sql"]

    if dry_run:
        print("DRY RUN — would execute:")
        for s in statements:
            print(f"  {s};")
        print(f"  + re-run {sql_files}")
        return

    with engine.begin() as conn:
        for stmt in statements:
            print(f"  {stmt}")
            conn.execute(text(stmt))

        for path in sql_files:
            print(f"  applying {path}")
            with open(path) as f:
                conn.execute(text(f.read()))

    print("Reset complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset CMS HealthFlow database")
    parser.add_argument("--confirm", action="store_true", help="Actually execute (default: dry-run)")
    args = parser.parse_args()

    if not args.confirm:
        print("This will DROP all schemas and recreate them. Pass --confirm to proceed.")
        reset(dry_run=True)
    else:
        reset(dry_run=False)


if __name__ == "__main__":
    main()
