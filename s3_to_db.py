import boto3
import json
from typing import Optional
from datetime import datetime

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
    key = 'news/' + today + '.json'
    print(key)
    bucket = 'economydataproject'
    try:
        data = get_data_from_s3(bucket, key)
        print(len(data))
    except Exception as e:
        print(e)