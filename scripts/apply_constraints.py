import yaml, psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

p = Path('profiles.yml')
conf = yaml.safe_load(p.read_text())
profile = conf.get('dvdrental', {})
outputs = profile.get('outputs', {})
dev = outputs.get('dev', {})
conn = psycopg2.connect(host=dev.get('host'), port=dev.get('port',5432), user=dev.get('user'), password=dev.get('password'), dbname=dev.get('dbname'))

schema='public_modelo_dimensional'

dims = {
    'dim_pelicula': 'id_pelicula',
    'dim_cliente': 'id_cliente',
    'dim_formato': 'id_formato',
    'dim_promocion': 'id_promocion',
    'dim_tiempo': 'id_tiempo'
}

fks = [
    ( 'hechos_alquileres', 'id_cliente', 'dim_cliente', 'id_cliente', 'fk_hechos_alquileres_cliente'),
    ( 'hechos_alquileres', 'id_pelicula', 'dim_pelicula', 'id_pelicula', 'fk_hechos_alquileres_pelicula')
]

# add missing FK relations: formato, promocion, tiempo
fks += [
    ('hechos_alquileres', 'id_formato', 'dim_formato', 'id_formato', 'fk_hechos_alquileres_formato'),
    ('hechos_alquileres', 'id_promocion', 'dim_promocion', 'id_promocion', 'fk_hechos_alquileres_promocion'),
    ('hechos_alquileres', 'id_tiempo', 'dim_tiempo', 'id_tiempo', 'fk_hechos_alquileres_tiempo')
]


def pk_exists(cur, table):
    cur.execute("""
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'p'
    """, (schema, table))
    return cur.fetchone() is not None


def constraint_exists(cur, table, conname):
    cur.execute("""
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = %s AND t.relname = %s AND c.conname = %s
    """, (schema, table, conname))
    return cur.fetchone() is not None


with conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # validate and create PKs
        for t, col in dims.items():
            print(f"Checking {schema}.{t} for duplicates on {col}...")
            cur.execute(f"SELECT {col}, count(*) as cnt FROM {schema}.{t} GROUP BY {col} HAVING count(*)>1 LIMIT 5;")
            dups = cur.fetchall()
            if dups:
                print(f"  DUPLICATES FOUND in {t} (showing up to 5):")
                for r in dups:
                    print('   ', r)
                print(f"  Aborting PK creation for {t} due to duplicates.")
                continue
            if pk_exists(cur, t):
                print(f"  PK already exists on {t}.")
            else:
                conname = f'pk_{t}'
                print(f"  Adding PK {conname} on {t}({col})...")
                cur.execute(f"ALTER TABLE {schema}.{t} ADD CONSTRAINT {conname} PRIMARY KEY ({col});")
                print(f"  PK added on {t}.")

        # create FKs
        for table, col, rtable, rcol, conname in fks:
            print(f"Ensuring FK {conname} on {schema}.{table}({col}) -> {schema}.{rtable}({rcol})...")
            # check referenced table has PK on rcol
            cur.execute("""
            SELECT c.conname, pg_get_constraintdef(c.oid) as def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'p'
            """, (schema, rtable))
            pk = cur.fetchone()
            if not pk:
                print(f"  Referenced table {rtable} has no PK; cannot add FK {conname}. Skipping.")
                continue
            # ensure fk doesn't already exist
            if constraint_exists(cur, table, conname):
                print(f"  FK {conname} already exists on {table}.")
                continue
            # check referential integrity: all values in table.col exist in rtable.rcol or are NULL
            cur.execute(f"SELECT count(*) as missing FROM {schema}.{table} t LEFT JOIN {schema}.{rtable} r ON t.{col} = r.{rcol} WHERE t.{col} IS NOT NULL AND r.{rcol} IS NULL;")
            miss = cur.fetchone()['missing']
            if miss and miss > 0:
                print(f"  Found {miss} orphan values in {table}.{col}; cannot add FK. Consider cleaning or allowing deferrable constraints.")
                continue
            # add fk
            print(f"  Adding FK {conname}...")
            cur.execute(f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {conname} FOREIGN KEY ({col}) REFERENCES {schema}.{rtable} ({rcol});")
            print(f"  FK {conname} added.")

print('\nDone')
conn.close()