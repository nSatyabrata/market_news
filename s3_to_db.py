import boto3
import json
import os
from typing import Optional
from datetime import datetime
from database import Database
from database_utilities import create_table_if_not_exist, insert_today_news_data
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = os.environ['DB_PORT']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

NOW = datetime.today()

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
    #today = datetime(year=2023, month=3, day=10).date().strftime('%d-%m-%y')
    key = 'news/' + today + '.json'
    print(key)
    bucket = 'economydataproject'
    try:
        data = get_data_from_s3(bucket, key)
        print(len(data))

        db = Database(host=DB_HOST,
                      database=DB_NAME,
                      user=DB_USER,
                      password=DB_PASSWORD,
                      port=DB_PORT)
    
        create_table_if_not_exist(db)
        insert_today_news_data(data, db)
        db.commit()
    except Exception as e:
        print(e)
    # try:
    #     db = Database(host="host", database="database", user="user", password="password")
    # except Exception as error:
    #     print(error)