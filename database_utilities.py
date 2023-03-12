from database import Database, DatabaseOperationError
from psycopg2.extras import execute_batch
from datetime import datetime


def create_table_if_not_exist(db: Database):

    # create table if not exists
    cursor = db.get_cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_news (
            id SERIAL,
            topic VARCHAR(100),
            title TEXT,
            url TEXT,
            summary TEXT,
            banner_image_url TEXT,
            source TEXT,
            date_created DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (id)
        );
    """)

def insert_today_news_data(data: dict, db: Database):
    """Insert news data in market_news table."""

    values = []
    for topic in data.keys():
        res = [(topic, v["title"], v["url"], v["summary"], v["banner_image"], v["source"]) for v in data[topic]]
        values += res
    
    values = tuple(values)

    cursor = db.get_cursor()
    sql_query = """
        INSERT INTO market_news (topic, 
                                 title, 
                                 url, 
                                 summary, 
                                 banner_image_url, 
                                 source)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_batch(cursor, sql_query, values, page_size=1000)


def delete_old_news_data(db: Database):
    """ Delete old data after inserting new data."""

    cursor = db.get_cursor()
    cursor.execute('''
        SELECT MAX(date_created) FROM market_news
    ''')
    max_date = cursor.fetchone()[0]
    today = datetime.today().date()

    if max_date == today:
        cursor.execute('''
            DELETE FROM market_news where date_created <> %s
        ''', (max_date,))
    else:
        raise DatabaseOperationError("New data is not available")