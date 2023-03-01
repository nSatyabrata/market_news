import requests, json, os, time
from dotenv import load_dotenv
from datetime import datetime, timedelta

now = datetime.today()
yesterday = now - timedelta(days=1)
now = now.strftime("%Y%m%dT%H%M")
yesterday = yesterday.strftime("%Y%m%dT%H%M")
print(now)
print(yesterday)
#YYYYMMDDTHHMM 
load_dotenv()

API_KEY = os.environ["API_KEY"]

topics = ["blockchain","earnings","ipo","mergers_and_acquisitions","financial_markets","economy_fiscal","economy_monetary","economy_macro","energy_transportation","finance","life_sciences","manufacturing","real_estate","retail_wholesale","technology"]

# API_URL = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={topics[1]}&apikey={API_KEY}&time_from={yesterday}&time_to={now}&sort=latest"
start = time.perf_counter()
# print(API_URL)
for topic in topics:
    s = time.perf_counter()
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={topic}&apikey={API_KEY}&time_from={yesterday}&time_to={now}&sort=latest"
    res = requests.get(url).json()
    if len(res) == 1:
        print(res)
    # Due to api call limit 5 request per 1 minute :
    # if total api call is greater than 5 and total time taken is less than 60 second
    # then wait for 1 min to complete then continue
    print(f"{topic}: {time.perf_counter() - s}")
print("Total: ",time.perf_counter() - start)
