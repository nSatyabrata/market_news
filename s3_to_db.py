import boto3
import json
import os
import logging
import sys
from typing import Optional
from datetime import datetime
from database import Database
from database_utilities import create_table_if_not_exist, insert_today_news_data, delete_old_news_data
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = os.environ['DB_PORT']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

NOW = datetime.today()


# logging setup
logger = logging.getLogger("News-Logger")
logger.setLevel(logging.INFO)

if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().handlers[0].setFormatter(
        logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')
    )
else:
    s_handler = logging.StreamHandler(sys.stdout)
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(
        logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')
    )
    logging.getLogger().addHandler(s_handler)


class S3GetObjectError(Exception):
    '''Custom exception for get data error.'''

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


def get_data_from_s3(bucket: str, key: str) -> Optional[dict]:
    '''Get data from s3 using bucket name and key'''

    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key= key)
        json_data = response['Body'].read().decode()
        data = json.loads(json_data)
        return data
    except Exception as e:
        raise S3GetObjectError(f"Couldn't get the data due to {e}")
    

if __name__ == "__main__":

    today = NOW.date().strftime('%d-%m-%y')
    key = 'news/' + today + '.json'
    bucket = 'economydataproject'
    try:
        data = get_data_from_s3(bucket, key)
        logger.info("S3 data fetch successful")

        db = Database(host=DB_HOST,
                      database=DB_NAME,
                      user=DB_USER,
                      password=DB_PASSWORD,
                      port=DB_PORT)

        logger.info("Database connection successful.")
        create_table_if_not_exist(db)
        insert_today_news_data(data, db)
        logger.info("Inserted new data.")
        delete_old_news_data(db)
        logger.info("Deleted old data.")
        db.commit()
        db.close()
    except Exception as error:
        logger.error(error)