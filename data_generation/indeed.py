import requests
from pprint import pprint
from dotenv import load_dotenv, find_dotenv
import os

class Indeed:
	def __init__(self, keywords):
		load_dotenv(find_dotenv())
		self.url = "https://indeed-scraper-api.p.rapidapi.com/api/job"


		self.payload = { "scraper": {
				"maxRows": 15,
				"query": keywords,
				"jobType": "fulltime",
				"sort": "relevance",
				"fromDays": "1",
				"country": "us"
			} }
		self.headers = {
			"x-rapidapi-key": os.getenv("x-rapidapi-key"),
			"x-rapidapi-host": "indeed-scraper-api.p.rapidapi.com",
			"Content-Type": "application/json"
		}

		self.jobs = self.fetch_jobs()

	def fetch_jobs(self):
		response = requests.post(self.url, json=self.payload, headers=self.headers)
		data = response.json()

		extracted_jobs = []

		for job in data.get("returnvalue", {}).get('data', []):
			try:
				salary_min = int(job.get('salary', {}).get('salaryMin', 0))
				salary_max = int(job.get('salary', {}).get('salaryMax', 0))
				salary_avg = (salary_min + salary_max) / 2 if salary_min and salary_max else None
			except Exception:
				salary_avg = None

			job_info = {
				'job_id': job.get('jobKey', 'N/A'),
				'title': job.get('title', 'N/A'),
				'location': job.get('location', {}).get('country', 'N/A'),
				'url': job.get('jobUrl', 'N/A'),
				'created': job.get('datePublished', 'N/A'),
				'skills': job.get('attributes', []),
				'salary': salary_avg
			}
			
			extracted_jobs.append(job_info)
		return extracted_jobs
	

# indeed_data = Indeed('data science').jobs
# pprint(indeed_data)
