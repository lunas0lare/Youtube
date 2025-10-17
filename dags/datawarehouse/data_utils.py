##contain functions relating to database connections and operations like createing tables and schema

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor #this helps return the data as a dict instead of a tuple from normal curror
from airflow.models import Variable
table = "yt_api"
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id = 'postgres_db_yt_elt', database = Variable.get('ELT_DATABASE_NAME'))
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID" VARCHAR (11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS{schema}.{table}(
                 "Video_ID" VARCHAR (11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
                );
                """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_id(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetch.all()
    video_ids = [row['Video_ID'] for row in ids]
    return video_ids