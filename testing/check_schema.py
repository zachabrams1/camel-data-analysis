import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to Railway database
conn = psycopg2.connect(
    host=os.getenv('PGHOST'),
    port=os.getenv('PGPORT'),
    database=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)

cursor = conn.cursor()

# Get all table names
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")
tables = [row[0] for row in cursor.fetchall()]

print("=== TABLES IN DATABASE ===")
print(", ".join(tables))
print("\n")

# For each table, get its schema
for table in tables:
    print(f"=== TABLE: {table} ===")

    # Get column information
    cursor.execute(f"""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position;
    """)

    columns = cursor.fetchall()
    for col in columns:
        col_name, data_type, max_len, nullable, default = col

        # Format the type
        if max_len:
            type_str = f"{data_type}({max_len})"
        else:
            type_str = data_type

        null_str = "NULL" if nullable == "YES" else "NOT NULL"
        default_str = f" DEFAULT {default}" if default else ""

        print(f"  {col_name}: {type_str} {null_str}{default_str}")

    # Get constraints
    cursor.execute(f"""
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        LEFT JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        LEFT JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.table_name = '{table}'
        AND tc.table_schema = 'public'
        ORDER BY tc.constraint_type, tc.constraint_name;
    """)

    constraints = cursor.fetchall()
    if constraints:
        print("\n  CONSTRAINTS:")
        for constraint in constraints:
            c_name, c_type, c_col, f_table, f_col, update_rule, delete_rule = constraint
            if c_type == 'PRIMARY KEY':
                print(f"    PRIMARY KEY: {c_col}")
            elif c_type == 'FOREIGN KEY':
                print(f"    FOREIGN KEY: {c_col} -> {f_table}({f_col}) [ON DELETE {delete_rule}]")
            elif c_type == 'UNIQUE':
                print(f"    UNIQUE: {c_col}")
            elif c_type == 'CHECK':
                print(f"    CHECK: {c_name}")

    print("\n")

cursor.close()
conn.close()
