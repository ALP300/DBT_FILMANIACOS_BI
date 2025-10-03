import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

p = Path(__file__).parent.parent / 'profiles.yml'
conf = yaml.safe_load(p.read_text())
profile = conf.get('dvdrental', {})
outputs = profile.get('outputs', {})
dev = outputs.get('dev', {})

conn = psycopg2.connect(
    host=dev.get('host'),
    port=dev.get('port', 5432),
    user=dev.get('user'),
    password=dev.get('password'),
    dbname=dev.get('dbname')
)

tables = ['promotion', 'rental', 'film', 'time', 'customer']
with conn:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for t in tables:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (t,))
            cols = cur.fetchall()
            print(f"\nTable: {t} - {len(cols)} columns")
            for c in cols:
                print(f"  {c['column_name']} ({c['data_type']})")

conn.close()
print('\nDone')
