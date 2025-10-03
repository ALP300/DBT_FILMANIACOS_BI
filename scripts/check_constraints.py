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
models=['dim_pelicula','dim_cliente','dim_formato','dim_promocion','dim_tiempo','hechos_alquileres']
with conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for m in models:
            cur.execute("""
            SELECT conname, contype, pg_get_constraintdef(c.oid) as definition
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = %s AND t.relname = %s
            """, (schema, m))
            rows = cur.fetchall()
            print(f"\nModel: {m} - constraints: {len(rows)}")
            for r in rows:
                print(' ', r['conname'], r['contype'], r['definition'])
conn.close()
print('\nDone')