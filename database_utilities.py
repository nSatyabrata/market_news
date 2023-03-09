import os
from database import Database


def create_market_news_table_if_not_exist(db: Database):
    db.query("""
        CREATE TABLE IF NOT EXISTS market_news (
            topic VARCHAR(100),
            title TEXT,
            url TEXT,
            summary TEXT,
            banner_image_url TEXT,
            source TEXT
        );
    """)


def insert_today_news_data(data: dict):
    pass


def delete_old_news_data():
    pass