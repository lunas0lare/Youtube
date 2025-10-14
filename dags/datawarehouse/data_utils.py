##contain functions relating to database connections and operations like createing tables and schema

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor #this helps return the data as a dict instead of a tuple from normal curror
from airflow.models import Variable

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id = 'postgres_db_yt_elt', database = Variable.get('ELT_DATABASE_NAME'))
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()