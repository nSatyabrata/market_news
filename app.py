import requests, json, os, time, logging, boto3, sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Optional, Dict, List

load_dotenv()

API_KEY = os.environ["API_KEY"]

NOW = datetime.today()
YESTERDAY = NOW - timedelta(days=1)

TOPICS = ["blockchain","earnings","ipo","mergers_and_acquisitions","financial_markets","economy_fiscal","economy_monetary","economy_macro","energy_transportation","finance","life_sciences","manufacturing","real_estate","retail_wholesale","technology"]

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


class S3UploadError(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)

def get_url_response(url: str) -> Optional[Dict]:
    try:
        response = requests.get(url)
        response.raise_for_status()
        response_data = response.json()
        return response_data
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to connect to api.") from e


def extract_news_data_of_topics(topics: List, time_from: str, time_to: str) -> str:
    data = {}
    time_elapsed = 0
    
    for i,topic in enumerate(topics):
        # 5 calls within a min
        if i!= 0 and i%5 == 0:
            time.sleep(60 - time_elapsed)
            time_elapsed = 0

        start = time.perf_counter()
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={topic}&apikey={API_KEY}&time_from={time_from}&time_to={time_to}&sort=latest"

        try:
            response = get_url_response(url)
            if response and response.get('items',0) != 0:
                data[topic] = response['feed'][:20]
        except Exception as error:
            raise error
        
        end = time.perf_counter()
        diff = end - start
        time_elapsed += diff

    data = json.dumps(data, indent=3)
    return data

def upload_data_s3(data: str , bucket:str, key:str):
    s3 = boto3.client('s3')
    try:
        s3.put_object(Body=data, Bucket=bucket, Key=key)
    except Exception as e:
        raise S3UploadError("Failed to upload in S3") from e
    

if __name__ == "__main__":

    # Date format for url: YYYYMMDDTHHMM 
    now = NOW.strftime("%Y%m%dT%H%M")
    yesterday = YESTERDAY.strftime("%Y%m%dT%H%M")
    try:
        # get news for all topics
        data = extract_news_data_of_topics(topics=TOPICS, time_from=yesterday, time_to=now)

        today = NOW.date().strftime('%d-%m-%y')
        key = 'news/' + today + '.json'
        bucket = 'economydataprojec'

        upload_data_s3(data, bucket, key)
        logger.info("Data extraction successful")
    except Exception as err:
        logger.error(f"{type(err)}, {err}")