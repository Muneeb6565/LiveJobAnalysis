import requests
import re
from bs4 import BeautifulSoup
import time
import random
import json
from data_generation.noun_extraction import noun
# from noun_extraction import noun
from dotenv import load_dotenv, find_dotenv
import os


class Adzuna:

    def __init__(self, keywords):
        load_dotenv(find_dotenv())


        self.keywords = keywords

        adzuna_id = os.getenv("adzuna_id")
        adzuna_key = os.getenv("adzuna_key")

        self.url = "https://api.adzuna.com/v1/api/jobs/us/search/1" 

        self.params = {
            'app_id' : adzuna_id,
            'app_key' : adzuna_key ,
            'what_phrase' : self.keywords,
            # "what_or": "data science machine learning data analytics data analyst",
            "max_days_old" : 7,
            "results_per_page": 30

        }
        self.jobs = self.fetch_jobs()

    
    # ---------------- Code for descripiton retrival ---------------------------

    def get_adzuna_description(self, id):
        page_url = 'https://www.adzuna.com/details/'
        response = requests.get(page_url + str(id))

        soup = BeautifulSoup(response.text, 'html.parser')
        description_div = soup.select_one('section.adp-body.mx-4.mb-4.text-sm.md\\:mx-0.md\\:text-base.md\\:mb-0')

        if description_div:
            raw_html = str(description_div)
            clean_text = re.sub(r'<[^>]+>', '', raw_html)  
            clean_text = re.sub(r'\s+', ' ', clean_text).strip() 
            return clean_text
        else:
            return "No description available."
    

    # Job extraction
    
    def fetch_jobs(self):
        print("Adzuna started")
        response = requests.get(url=self.url, params=self.params)

        extracted_jobs = []

        
        if response.status_code == 200:
            data = response.json()
            
            for job in data.get('results', []):

                description  = job.get("description", "")

                if description:
                    skills = noun(description).result

                    job_id = job.get('id', 'N/A')
                    title = job.get('title', 'N/A')
                    location = job.get('location', 'N/A')
                    created = job.get('created', 'N/A')
                    url = job.get('redirect_url', 'N/A')
                    # description  = self.get_adzuna_description(job_id)
                    # skills = noun(description).result
                    # description  = job.get("description", "")
                    try :
                        salary_min = int(job.get('salary_min', 'N/A'))
                        salary_max = int(job.get('salary_max', 'N/A'))
                        salary = (salary_min + salary_max)/2
                    except:
                        salary = 0

                    # time.sleep(random.uniform(5, 10))
                    
                    job_info = {
                        'job_id': job_id,
                        'title': title,
                        # 'location': location,
                        'location': location['area'][0],
                        'created': created,
                        'url': url,
                        'description': description,
                        'skills' : skills,
                        'salary' : salary
                    }

                    extracted_jobs.append(job_info)
                else:
                    print("No description")

            return extracted_jobs


# obj  = Adzuna('data science').jobs
# print(obj)












