import requests
from data_generation.noun_extraction import noun
# from noun_extraction import noun
from pprint import pprint
from dotenv import load_dotenv, find_dotenv
import os

class LinkedIn:
    def __init__(self, keywords):
        load_dotenv(find_dotenv())
        self.keywords = keywords

        self.url = "https://linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com/jobs/search"

        self.querystring = {"keywords":self.keywords,"location":"United States","page_number":"1"}

        self.headers = {
            "x-rapidapi-key": os.getenv("x-rapidapi-key"),
            "x-rapidapi-host": "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"
        }
        self.jobs = self.fetch_jobs()



    def fetch_jobs(self):
        print("linkedin started")

        response = requests.get(self.url, headers=self.headers, params=self.querystring)
        data = response.json() 

        extracted_jobs = []
        for job in data['data']['jobs']:

            try:
                desc = job.get('description', 'N/A')
                job_info = {
                    
                    'job_id': job.get('job_id', 'N/A'),
                    'title': job.get('job_title', 'N/A'),
                    'location': job.get('location', 'N/A'),
                    'created': job.get('created_at', 'N/A'),
                    'description': desc,
                    'skills' : noun(desc).result,
                    'url': job.get('job_url', 'N/A'),
                    'salary' : job.get('salary', 'N/A')
                }
                extracted_jobs.append(job_info)
            except Exception as e:
                pass 
        return extracted_jobs



# obj = LinkedIn('data science')
# pprint(obj.jobs)

