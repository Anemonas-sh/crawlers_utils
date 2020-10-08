import os
from datetime import datetime, timedelta

days_to_query_from_now = 1

start_date = datetime.now()
end_date = datetime.now() + timedelta(days=days_to_query_from_now)
print(start_date, end_date)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "datatour-cra-f794120ccd2f.json"
crawlers_paths = ["./../google-flights-crawler/google_flights_crawler.py",
                  "./../expedia-hotels-crawler/expedia_crawler.py",
                  "./../airbnb-crawler-api/airbnb_crawler.py"]
estimate_level = 0
threads = 8
for crawler in crawlers_paths:
    os.system("python3 %s --start-date %s --end-date %s --estimate-level %d --threads %d" %
               (crawler_name, start_date, end_date, estimate_level, threads))

turn_off_vm = """
az logout
az login --identity
az vm deallocate -g datatour-rg -n Datatour-Crawlers
"""

os.system(turn_off_vm)