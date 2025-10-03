import yaml, psycopg2
from pathlib import Path
p = Path('profiles.yml')
conf = yaml.safe_load(p.read_text())
profile = conf.get('dvdrental', {})
outputs = profile.get('outputs', {})
dev = outputs.get('dev', {})
conn = psycopg2.connect(host=dev.get('host'), port=dev.get('port',5432), user=dev.get('user'), password=dev.get('password'), dbname=dev.get('dbname'))
schema='public_modelo_dimensional'
table='dim_pelicula'
try:
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'p'
              ) THEN
                ALTER TABLE {schema}.{table} ADD CONSTRAINT pk_dim_pelicula PRIMARY KEY (id_pelicula);
              END IF;
            END
            $$;
            """, (schema, table))
            print('PK ensured on', f'{schema}.{table}')
except Exception as e:
    print('Error:', e)
finally:
    conn.close()
