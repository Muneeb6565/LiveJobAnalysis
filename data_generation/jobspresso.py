import time
import requests
import xml.etree.ElementTree as ET
import re
from data_generation.noun_extraction import noun
import json
from pprint import pprint


class Jobspresso:
    # Job categories and their RSS feed URLs
    check_boxes = {
        'ai_&_data': 'https://jobspresso.co/?feed=job_feed&job_types=ai-data&search_location&job_categories&search_keywords',
        'software_development': 'https://jobspresso.co/?feed=job_feed&job_types=developer&search_location&job_categories&search_keywords',
        'product_management_jobs': 'https://jobspresso.co/?feed=job_feed&job_types=product-mgmt&search_location&job_categories&search_keywords',
        'design_&_ux': 'https://jobspresso.co/?feed=job_feed&job_types=designer&search_location&job_categories&search_keywords',
        'customer-service-jobs': 'https://jobspresso.co/?feed=job_feed&job_types=support&search_location&job_categories&search_keywords',
        'marketing': 'https://jobspresso.co/?feed=job_feed&job_types=marketing&search_location&job_categories&search_keywords',
        'sales': 'https://jobspresso.co/?feed=job_feed&job_types=sales&search_location&job_categories&search_keywords',
        'writing-jobs': 'https://jobspresso.co/?feed=job_feed&job_types=writing&search_location&job_categories&search_keywords',
        'non-tech-jobs': 'https://jobspresso.co/?feed=job_feed&job_types=various&search_location&job_categories&search_keywords' 
    }

    namespaces = {'content': 'http://purl.org/rss/1.0/modules/content/'}

    def __init__(self, category='ai_&_data'):
        self.url = self.check_boxes[category]

    @staticmethod
    def clean_description(raw_html):
        text = re.sub(r'<.*?>', '', raw_html)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def get_jobs(self):
        print('jobpresso started')
        response = requests.get(self.url)
        root = ET.fromstring(response.content)
        jobs = []

        for item in root.findall('./channel/item'):
            job_id = item.findtext('guid')
            title = item.findtext('title')
            company = item.findtext('{http://jobspresso.co}company')
            location = item.findtext('{http://jobspresso.co}location')
            job_url = item.findtext('link')
            posted_date = item.findtext('pubDate')

            desc_elem = item.find('content:encoded', self.namespaces)
            raw_html_desc = desc_elem.text if desc_elem is not None else ''
            cleaned_desc = self.clean_description(raw_html_desc)

            skills = noun(cleaned_desc).result

            job = {
                'job_id': job_id,
                'title': title,
                # 'company': company,
                'location': location,
                'url': job_url,
                'created': posted_date,
                'description': cleaned_desc,
                'skills' : skills,
                'salary' : None
            }

            jobs.append(job)

        
        return jobs


# obj = Jobspresso(category='ai_&_data').get_jobs()
# pprint(obj)

