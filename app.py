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

data = {}

s = time.perf_counter()
time_elapsed = 0
# print(API_URL)
for i,topic in enumerate(topics):
    # 5 calls within a min
    if i!= 0 and i%5 == 0:
        time.sleep(60 - time_elapsed)
        print(f"Pausing for {60 - time_elapsed}")
        time_elapsed = 0

    start = time.perf_counter()
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={topic}&apikey={API_KEY}&time_from={yesterday}&time_to={now}&sort=latest"
    response = requests.get(url).json()
    
    if len(response) == 1:
        print(response)
    else:
        if int(response['items']) > 20:
            data[topic] = response['feed'][:20]
        else:
            data[topic] = response['feed']
        
    end = time.perf_counter()
    diff = end - start
    time_elapsed += diff
    print(f"{topic,i}: {diff}")
print(len(data))

with open('market_news.json', "w") as f:
    json.dump(data, f, indent=4)

print("Total: ",time.perf_counter() - s)
