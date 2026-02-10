#!/usr/bin/env python3
"""
Export Railway PostgreSQL database schema to a local SQL file.
This creates a schema.sql file that tracks the current state of all tables.

Usage:
    python export_schema.py
    
The script will connect to Railway using credentials from .env and export
the complete schema to schema.sql in the project root.
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def connect_to_db():
    """Connect to the Railway PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=os.getenv('PGPORT'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )


def get_tables(cursor):
    """Get all table names in public schema, ordered by dependencies."""
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    return [row[0] for row in cursor.fetchall()]


def get_columns(cursor, table_name):
    """Get column definitions for a table."""
    cursor.execute(f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            udt_name
        FROM information_schema.columns
        WHERE table_name = %s
        AND table_schema = 'public'
        ORDER BY ordinal_position;
    """, (table_name,))
    return cursor.fetchall()


def get_primary_key(cursor, table_name):
    """Get primary key columns for a table."""
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = %s
        AND tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
        ORDER BY kcu.ordinal_position;
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def get_foreign_keys(cursor, table_name):
    """Get foreign key constraints for a table."""
    cursor.execute("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column,
            rc.delete_rule,
            rc.update_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.table_name = %s
        AND tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public';
    """, (table_name,))
    return cursor.fetchall()


def get_unique_constraints(cursor, table_name):
    """Get unique constraints for a table."""
    cursor.execute("""
        SELECT tc.constraint_name, string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = %s
        AND tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = 'public'
        GROUP BY tc.constraint_name;
    """, (table_name,))
    return cursor.fetchall()


def get_check_constraints(cursor, table_name):
    """Get check constraints for a table."""
    cursor.execute("""
        SELECT cc.constraint_name, cc.check_clause
        FROM information_schema.check_constraints cc
        JOIN information_schema.table_constraints tc
            ON cc.constraint_name = tc.constraint_name
            AND cc.constraint_schema = tc.table_schema
        WHERE tc.table_name = %s
        AND tc.table_schema = 'public'
        AND tc.constraint_type = 'CHECK'
        AND cc.constraint_name NOT LIKE '%%_not_null';
    """, (table_name,))
    return cursor.fetchall()


def format_column_type(col):
    """Format column type from column info."""
    col_name, data_type, char_max_len, num_precision, num_scale, is_nullable, default, udt_name = col
    
    # Handle serial types (detected by default containing 'nextval')
    if default and 'nextval' in str(default):
        if data_type == 'integer':
            return 'SERIAL'
        elif data_type == 'bigint':
            return 'BIGSERIAL'
        elif data_type == 'smallint':
            return 'SMALLSERIAL'
    
    # Handle array types
    if data_type == 'ARRAY':
        return f"{udt_name.replace('_', '')}[]"
    
    # Handle character types
    if data_type == 'character varying':
        if char_max_len:
            return f"VARCHAR({char_max_len})"
        return "VARCHAR"
    elif data_type == 'character':
        if char_max_len:
            return f"CHAR({char_max_len})"
        return "CHAR"
    
    # Handle numeric with precision
    if data_type == 'numeric' and num_precision:
        if num_scale:
            return f"NUMERIC({num_precision}, {num_scale})"
        return f"NUMERIC({num_precision})"
    
    # Map common types
    type_map = {
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'boolean': 'BOOLEAN',
        'text': 'TEXT',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMPTZ',
        'date': 'DATE',
        'time without time zone': 'TIME',
        'json': 'JSON',
        'jsonb': 'JSONB',
        'uuid': 'UUID',
        'numeric': 'NUMERIC',
    }
    
    return type_map.get(data_type, data_type.upper())


def generate_create_table(cursor, table_name):
    """Generate CREATE TABLE statement for a table."""
    columns = get_columns(cursor, table_name)
    pk_cols = get_primary_key(cursor, table_name)
    fks = get_foreign_keys(cursor, table_name)
    uniques = get_unique_constraints(cursor, table_name)
    checks = get_check_constraints(cursor, table_name)
    
    lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
    col_defs = []
    
    for col in columns:
        col_name, data_type, char_max_len, num_precision, num_scale, is_nullable, default, udt_name = col
        
        col_type = format_column_type(col)
        
        # Build column definition
        col_def = f"    {col_name} {col_type}"
        
        # Add NOT NULL (skip for SERIAL types which are auto NOT NULL)
        if is_nullable == 'NO' and 'SERIAL' not in col_type:
            col_def += " NOT NULL"
        
        # Add DEFAULT (skip for SERIAL types)
        if default and 'nextval' not in str(default) and 'SERIAL' not in col_type:
            col_def += f" DEFAULT {default}"
        
        col_defs.append(col_def)
    
    # Add PRIMARY KEY constraint
    if pk_cols:
        if len(pk_cols) == 1:
            # Find and modify the column definition to include PRIMARY KEY
            for i, col_def in enumerate(col_defs):
                if col_def.strip().startswith(pk_cols[0] + ' '):
                    col_defs[i] += " PRIMARY KEY"
                    break
        else:
            col_defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")
    
    # Add UNIQUE constraints
    for constraint_name, cols in uniques:
        col_defs.append(f"    UNIQUE ({cols})")
    
    # Add CHECK constraints
    for constraint_name, check_clause in checks:
        col_defs.append(f"    CHECK {check_clause}")
    
    # Add FOREIGN KEY constraints
    for fk in fks:
        constraint_name, col, foreign_table, foreign_col, delete_rule, update_rule = fk
        fk_def = f"    CONSTRAINT {constraint_name}\n        FOREIGN KEY ({col})\n        REFERENCES {foreign_table}({foreign_col})"
        if delete_rule and delete_rule != 'NO ACTION':
            fk_def += f"\n        ON DELETE {delete_rule}"
        if update_rule and update_rule != 'NO ACTION':
            fk_def += f"\n        ON UPDATE {update_rule}"
        col_defs.append(fk_def)
    
    lines.append(',\n'.join(col_defs))
    lines.append(");")
    
    return '\n'.join(lines)


def get_indexes(cursor, table_name):
    """Get non-primary/non-unique indexes for a table."""
    cursor.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = %s
        AND schemaname = 'public'
        AND indexdef NOT LIKE '%%UNIQUE%%'
        AND indexname NOT LIKE '%%_pkey';
    """, (table_name,))
    return cursor.fetchall()


def export_schema():
    """Export the complete database schema to schema.sql."""
    print("Connecting to Railway database...")
    conn = connect_to_db()
    cursor = conn.cursor()
    print("✅ Connected successfully!")
    
    tables = get_tables(cursor)
    print(f"Found {len(tables)} tables: {', '.join(tables)}")
    
    # Build schema content
    schema_lines = [
        f"-- Railway Database Schema",
        f"-- Exported on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"-- Database: {os.getenv('PGDATABASE')}",
        f"-- Host: {os.getenv('PGHOST')}",
        f"--",
        f"-- This file tracks the current state of the database schema.",
        f"-- Re-run export_schema.py to update this file after making changes.",
        "",
        ""
    ]
    
    # Generate CREATE TABLE for each table
    for table in tables:
        schema_lines.append(f"-- ============================================")
        schema_lines.append(f"-- TABLE: {table}")
        schema_lines.append(f"-- ============================================")
        schema_lines.append("")
        schema_lines.append(generate_create_table(cursor, table))
        
        # Add indexes
        indexes = get_indexes(cursor, table)
        if indexes:
            schema_lines.append("")
            for idx_name, idx_def in indexes:
                schema_lines.append(f"{idx_def};")
        
        schema_lines.append("")
        schema_lines.append("")
    
    # Write to file
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    with open(schema_path, 'w') as f:
        f.write('\n'.join(schema_lines))
    
    print(f"\n✅ Schema exported to: {schema_path}")
    print(f"   Total tables: {len(tables)}")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    export_schema()
