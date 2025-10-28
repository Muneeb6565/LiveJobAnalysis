from dotenv import load_dotenv
import os
from supabase import create_client
from pprint import pprint
import pandas as pd


load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_API)

df_skills = supabase.table('job_skill_view').select('*').execute()
df = pd.DataFrame(df_skills.data)
print(df.columns)