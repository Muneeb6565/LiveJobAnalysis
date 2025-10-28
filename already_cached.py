from supabase import create_client
from pipeline2 import JobPipeline
from analyzation import AnalyzationPipeline
from skill_analyzation import WilsonNecessityWidget
from database_insertion import Database
import pandas as pd
import time
from dotenv import load_dotenv
import os

def upload_to_supabase():
    load_dotenv()
    print("Already cached process started")

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    analyzer = AnalyzationPipeline()

    db = Database(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        table_skills="skills",
        table_jobs="jobs",
        table_job_skills="job_skills",
    )

    # keywords = [
    #     "data science", "Machine Learning", "Generative AI", "Data Engineering",
    #     "Business Intelligence", "Backend Engineering", "Frontend Engineering",
    #     "Full-Stack Engineering", "DevOps  SRE  Cloud Engineering", 
    #     "Cybersecurity", "Mobile Development", "QA / Test Automation", 
    #     "Product Management", "UX / UI Design"
    # ]

    keywords = ["Full-Stack Engineering", "data science"]  
    
    for idx, kw in enumerate(keywords, start=1):
        print(f"\n🔄 Processing {idx}/{len(keywords)}: '{kw}'")

        existing_check = supabase.table("cached").select("name").eq("name", kw).execute()
        if existing_check.data:
            print(f"🗑️  Deleting existing record for '{kw}'")
            delete_response = supabase.table("cached").delete().eq("name", kw).execute()
            time.sleep(1)

            print(f"   Deleted {len(delete_response.data)} record(s)")
        else:
            print(f"ℹ️  No existing record found for '{kw}' - creating new")

        pipeline = JobPipeline(keyword=kw, supabase_url=SUPABASE_URL, supabase_api=SUPABASE_KEY)
        print(f"📡 Fetching job data for '{kw}'...")
        pipeline.fetch_data()
        df_skills = pipeline.extract_skills()

        print("💾 Processing database operations...")
        skills_unique, jobs_table, job_skills_name_only = db.fill_tables(df_skills)
        skills_map_df, jobs_table_final, job_skills_full = db.insert_into_supabase(
            skills_unique, jobs_table, job_skills_name_only
        )

        skills_res = supabase.table("skills").select("SkillId, SkillName").execute()
        skills_df = pd.DataFrame(skills_res.data or [])

        page_size = 1000
        page = 0
        all_rows = []

        while True:
            start = page * page_size
            end   = start + page_size - 1
            resp = (
                supabase
                .table("job_skill_view")
                .select("JobId,Title,JobPosted,Keyword,SkillName")
                .eq("Keyword", kw)
                .order("JobPosted", desc=True)
                .range(start, end)
                .execute()
            )
            batch = resp.data or []
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            page += 1

        jobs_df = pd.DataFrame(all_rows)
        print(jobs_df.head())

        # # Step 3: Generate analysis plots
        print(f"📊 Generating analysis plots...")

        plt1 = analyzer.analyze_top_skills(df_skills, analyze=True)
        plt2, skills_list = analyzer.skill_trends()

        # Wilson necessity analysis
        widget = WilsonNecessityWidget(df_skills, nec_wlb_pct=40.0)
        _ = widget.run()
        plt3 = widget.plot_base64()

        # Step 4: Prepare new record
        row = {
            "name": kw,
            "plt1": plt1,
            "plt2": plt2,
            "plt3": plt3,
            "skill_list": skills_list,
        }

        # Step 5: Insert new record
        print(f"⬆️  Uploading new data for '{kw}'...")
        response = supabase.table("cached").insert(row).execute()
        time.sleep(5)

        if response.data:
            print(f"✅ Successfully uploaded '{kw}': Record created")
            print(f"   Skills analyzed: {len(skills_list) if skills_list else 0}")
            print(f"   Plots generated: 3")
        else:
            print(f"⚠️  Upload completed but no data returned for '{kw}'")


    print(f"\n🎉 Processing complete! Processed {len(keywords)} keywords.")


def clean_cached_table():
    load_dotenv()


    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Get all records first
    all_records = supabase.table("cached").select("name").execute()
    print(f"Found {len(all_records.data)} records in cached table")
    
    # Delete all records
    if all_records.data:
        for record in all_records.data:
            delete_response = supabase.table("cached").delete().eq("name", record["name"]).execute()
            print(f"Deleted record: {record['name']}")


def update_single_keyword(keyword):
    load_dotenv()


    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    print(f"🎯 Single keyword update for: '{keyword}'")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    analyzer = AnalyzationPipeline()

    db = Database(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        table_skills="skills",
        table_jobs="jobs", 
        table_job_skills="job_skills",
    )

    # try:
        # Delete existing record
    existing_check = supabase.table("cached").select("name").eq("name", keyword).execute()
    if existing_check.data:
        print(f"🗑️  Deleting existing record for '{keyword}'")
        supabase.table("cached").delete().eq("name", keyword).execute()

    # Process and upload new data
    pipeline = JobPipeline(keyword=keyword, supabase_url=SUPABASE_URL, supabase_api=SUPABASE_KEY)
    pipeline.fetch_data()
    df_skills = pipeline.extract_skills()
    # df = df_skills
    print(df_skills.columns)

    skills_unique, jobs_table, job_skills_name_only = db.fill_tables(df_skills)
    db.insert_into_supabase(skills_unique, jobs_table, job_skills_name_only)

    plt1 = analyzer.analyze_top_skills(df_skills)
    plt2, skills_list = analyzer.skill_trends()

    widget = WilsonNecessityWidget(df_skills, nec_wlb_pct=40.0)
    _ = widget.run()
    plt3 = widget.plot_base64()

    row = {
        "name": keyword,
        "plt1": plt1,
        "plt2": plt2,
        "plt3": plt3,
        "skill_list": skills_list,
    }
    
    response = supabase.table("cached").upsert(row).execute()
    print(f"✅ Successfully updated '{keyword}'")


# if __name__ == "__main__":

    # update_single_keyword("data science")  # Update single keyword

    # upload_to_supabase()
    # clean_cached_table() 