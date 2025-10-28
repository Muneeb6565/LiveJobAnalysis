from flask import Flask, render_template, request, current_app, render_template_string, jsonify, abort
from flask_apscheduler import APScheduler
from pipeline2 import JobPipeline
from analyzation import AnalyzationPipeline
from skill_analyzation import WilsonNecessityWidget
from roadmap import GPTToolExtractor
from supabase import create_client 
import time
import logging
from already_cached import upload_to_supabase as cached_upload_to_supabase
from database_insertion import Database
from dotenv import load_dotenv
import os
import pandas as pd
 

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_API)


analyzation_pipeline = AnalyzationPipeline()


db = Database(
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_API,
    table_skills="skills",
    table_jobs="jobs",
    table_job_skills="job_skills",
)



app = Flask(__name__)

# *****************************************************************************************************

logging.basicConfig(level=logging.INFO)

ADMIN_UPLOAD_TOKEN = os.getenv("ADMIN_UPLOAD_TOKEN", "change_me")

# -----------------------
# Scheduled job function
# -----------------------
def upload_to_supabase():

    try:
        app.logger.info("Starting upload_to_supabase()")

        print("caching started")
        cached_upload_to_supabase()

        app.logger.info("upload_to_supabase() completed successfully")
        return "ok"
    
    except Exception as e:
        app.logger.exception("upload_to_supabase() failed")
        return f"error: {e}"

# -----------------------
# APScheduler config & start
# -----------------------
class Config:
    SCHEDULER_API_ENABLED = True 

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)

scheduler.add_job(
    id="daily_supabase_upload",
    func=upload_to_supabase,
    trigger="cron",
    hour=16,
    minute=14,
)

def _start_scheduler_once():
    """
    Avoid starting scheduler twice in debug reloader.
    """
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        if not getattr(app, "_scheduler_started", False):
            scheduler.start()
            app._scheduler_started = True
            app.logger.info("APScheduler started")



# Manual trigger endpoint (optional)
@app.route("/admin/upload", methods=["POST", "GET"])
def manual_upload():
    token = request.args.get("token")
    if token != ADMIN_UPLOAD_TOKEN:
        abort(403)
    result = upload_to_supabase()
    return jsonify({"status": result})



# *****************************************************************************************************
@app.route('/', methods=['GET', 'POST'])
def func():
    if request.method == 'POST':
        if request.form.get('r') : 
            selected_role = request.form.get('role')  
            print(selected_role.lower())
            response = supabase.table('cached').select("*").eq('name', selected_role).execute()
            row = response.data

            payload = {
                "frequent_skills_plot": row[0]['plt1'],
                "skill_trend_plot": row[0]['plt2'],
                "necessary_vs_better_plot": row[0]['plt3'],
            }
            skill_list = row[0]['skill_list']
            skill_list = skill_list.strip("[]")
            skill_list = [item.strip() for item in skill_list.split(",")]

            
            return render_template('check.html', keyword=payload, skills_list=skill_list)


        keyword = (request.form.get('q') or '').strip()
        print(keyword)

        if not keyword:
            return render_template('check.html', keyword=None, skills_list=[], error="Please enter a keyword or click a role.")

        if request.form.get('q'):

            pipeline = JobPipeline(
                        keyword= keyword,
                        supabase_url=SUPABASE_URL,
                        supabase_api= SUPABASE_API,
                    )


            pipeline.fetch_data()
            df_skills = pipeline.extract_skills()
            df_skills = df_skills[df_skills.skills != "There are no technical tools, programming languages, or software relevant to jobs in the provided list."]
            skills_unique, jobs_table, job_skills_name_only = db.fill_tables(df_skills)
            skills_map_df, jobs_table_final, job_skills_full = db.insert_into_supabase(skills_unique, jobs_table, job_skills_name_only)

            time.sleep(5)

            page_size = 1000 
            all_data = []

            for i in range(4): 
                offset = i * page_size
                
                batch = (
                    supabase.table("job_skill_view")
                    .select("*")
                    .eq("Keyword", keyword)
                    .order("JobId")
                    .range(offset, offset + page_size - 1) 
                    .execute()
                )
                
                all_data.extend(batch.data)
                if len(batch.data) < page_size:
                    break


            df_skills = pd.DataFrame(all_data)
            df_skills = df_skills.rename(columns={
                'JobId': 'job_id',
                'Title': 'title',
                'SkillName': 'skills',
                'Keyword': 'keyword',
                'JobPosted': 'created'
            })

            frequent_skills_plot = analyzation_pipeline.analyze_top_skills(df_skills)
            skill_trend_plot, skill_list = analyzation_pipeline.skill_trends()


            widget = WilsonNecessityWidget(df_skills, nec_wlb_pct=40.0)
            _ = widget.run()
            necessary_vs_better_plot = widget.plot_base64()

            payload = {
                "frequent_skills_plot": frequent_skills_plot,
                "skill_trend_plot": skill_trend_plot,
                "necessary_vs_better_plot": necessary_vs_better_plot,
            }

            return render_template('check.html', keyword=payload, skills_list=skill_list)
    return render_template('check.html', keyword=None, skills_list=[])



@app.route('/roadmap', methods=['POST'])
def roadmap():
    selected_skills = request.form.getlist('skills')
    duration = request.form.get("duration") 
    extractor = GPTToolExtractor(str(duration),  selected_skills)
    return render_template_string(extractor.result)



@app.route('/about_us', methods=['GET', 'POST'])
def func2():
    return render_template('about_us.html')


@app.route('/contact_us')
def func3():
    return render_template('contact_us.html')


@app.route('/check', methods=['GET', 'POST'])
def func4():
    return render_template('check.html')


if __name__ == '__main__':
   _start_scheduler_once()
   app.run(debug = True)