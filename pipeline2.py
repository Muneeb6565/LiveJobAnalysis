from data_generation import linkedin, adzuna, indeed, jobspresso
import time
import pandas as pd
from gpt_tool_extraction import GPTToolExtractor
from supabase import create_client
from pprint import pprint


class JobPipeline:

    def __init__(self, keyword, supabase_url, supabase_api):
        self.keyword = keyword
        self.supabase_url = supabase_url
        self.supabase_api = supabase_api
        self.df = None
        self.skills_table = None
        self.jobs_table = None
        self.job_skills_table = None
        self.response = None


    def fetch_data(self):
        print("function called")
        adzuna_data = pd.DataFrame(adzuna.Adzuna(self.keyword).jobs)
        pprint(len(adzuna_data))
       
        linkedin_data = pd.DataFrame(linkedin.LinkedIn(self.keyword).jobs)
        print("LinkedIn done")
        print(len(linkedin_data))

        indeed_data = pd.DataFrame(indeed.Indeed(self.keyword).jobs)
        print(len(indeed_data))
        print("Indeed done")

        jobspresso_data = pd.DataFrame(jobspresso.Jobspresso(category='ai_&_data').get_jobs())
        print(jobspresso_data.columns)
        print(len(jobspresso_data))
        print("Jobspresso done")

        self.df = pd.concat([adzuna_data, linkedin_data, indeed_data, jobspresso_data], ignore_index=True)
        self.df['keyword'] = self.keyword
        pprint(self.df)
        print(f"Total jobs fetched: {len(self.df)}")
        print("Sample columns:", self.df.columns.tolist())
        # return self.df
        return None
        



    def extract_skills(self):
        if self.df is None:
            raise ValueError("DataFrame is empty. Call fetch_data() first.")
        
        # self.df = pd.read_csv("merged_updated.csv")
        for i in range(len(self.df)):
            skill_text = self.df.loc[i, 'skills']
            extracted = GPTToolExtractor(skill_text).result
            self.df.at[i, 'skills'] = extracted
            time.sleep(1)
        
        # self.df.to_csv("merged.csv", index=False)
        # print("Skills extracted and saved to merged_updated.csv")

        return self.df  
    

