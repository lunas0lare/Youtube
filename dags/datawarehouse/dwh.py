from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_id
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_modification import update_row, insert_row, delete_rows
from datawarehouse.data_loading import load_data

import logging
from airflow.decorators import task

logger = logging.getLogger("__name__")
table = "yt_api"

@task
def staging_table():
    schema = 'staging'

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_id(cur, schema)

        for row in YT_data:
            #insert data into a new table
            if len(table_ids) == 0:
                insert_row(cur, conn, schema, row)

            #insert data into a table that already have data
            else:
                if row['video_id'] in table_ids:
                    update_row(cur, conn, schema, row)
                else:
                    #newe video inserted in table
                    insert_row(cur, conn, schema, row)
        
        #process deleted videos
        ids_in_json = {row['video_id'] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"{schema} table update completed")
    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
    

@task
def core_table():
    schema = 'core'

    conn, cur = None, None
    
    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)
        
        table_ids = get_video_id(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()
        #if process with large data, fetch them by batch(fetchmany(size))

        for row in rows:

            current_video_ids.add(row['Video_ID'])
            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_row(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)

                if transformed_row['Video_ID'] in table_ids:
                    update_row(cur, conn, schema, transformed_row)
                else:
                    insert_row(cur, conn, schema, transformed_row)
                  
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table; {e}")
        raise e

    finally:
        if conn and cur:   
            close_conn_cursor(conn, cur)