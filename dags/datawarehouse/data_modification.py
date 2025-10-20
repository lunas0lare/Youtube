import logging

logger = logging.getLogger("__name__")
table = 'yt_api'

#reason for 2 different video_id is because the first we read from the data, which video_id is the same as variable,
#but the else(cored layer) we read data from the created table so video_id is read from our table.
def insert_row(cur, conn, schema, row):
    try:
        if schema == 'staging':

            video_id = 'video_id'#for logging purposes

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);""", row,#(%(name)s) is how to insert values
                )
        else:
            video_id = 'Video_ID'

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s,  %(Comments_Count)s)""", row)
            
        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id]}")
    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e
    
def update_row(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title ='title'
            video_views ='viewCount'
            likes_count ='likeCount'
            comments_count ='commentCount'
        else:
            video_id = 'Video_ID'
            upload_date = 'Upload_Date'
            video_title = ' Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'
        
        
        cur.execute(
            #these changes day to day so we need to update
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """, row
        )

        conn.commit()
    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e 
    
def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({(', ').join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" in {ids_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Deleted rows with Video_ID: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e

