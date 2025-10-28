import os
import ast
import pandas as pd
from typing import Iterable, Tuple, Optional
from supabase import create_client, Client
from postgrest.exceptions import APIError


class Database:
    """
    Import-friendly Supabase database helper for your jobs/skills pipeline.

    Usage:
        from db_pipeline import Database
        db = Database(
            supabase_url=os.getenv("SUPABASE_URL"),
            supabase_key=os.getenv("SUPABASE_KEY"),
            table_skills="skills",
            table_jobs="jobs",
            table_job_skills="job_skills",
        )
        skills_unique, jobs_table, job_skills_name_only = db.fill_tables(df)
        skills_map_df, jobs_table_final, job_skills_full = db.insert_into_supabase(
            skills_unique, jobs_table, job_skills_name_only
        )
    """

    SENTINEL = (
        "There are no technical tools, programming languages, or software "
        "relevant to jobs in the provided list."
    )

    def __init__(
        self,
        supabase_url: Optional[str] = None,
        supabase_key: Optional[str] = None,
        *,
        table_skills: str = "skills",
        table_jobs: str = "jobs",
        table_job_skills: str = "job_skills",
        client: Optional[Client] = None,
    ) -> None:
        """
        If `client` is provided, it will be used directly (good for testing).
        Otherwise, a new Supabase Client will be created from URL/KEY.
        """
        self.table_skills = table_skills
        self.table_jobs = table_jobs
        self.table_job_skills = table_job_skills

        if client is not None:
            self.sb: Client = client
        else:
            # Prefer environment variables; fall back to explicit args if passed
            url = supabase_url or os.getenv("SUPABASE_URL")
            key = supabase_key or os.getenv("SUPABASE_KEY")

            if not url or not key:
                raise ValueError(
                    "Supabase URL/KEY not provided. Set SUPABASE_URL and SUPABASE_KEY env vars "
                    "or pass them to Database(...)."
                )
            self.sb = create_client(url, key)

    # ----------------------------
    # Public API
    # ----------------------------
    def fill_tables(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transform your raw dataframe into:
         1) skills_unique (SkillName)
         2) jobs_table (JobId, Title, JobLink, JobPosted, Keyword)
         3) job_skills_name_only (JobId, SkillName)
        """
        df = df.loc[df["skills"].notna() & (df["skills"] != self.SENTINEL)].copy()
        df["skills"] = df["skills"].apply(self._to_list)

        exploded = df.explode("skills").dropna(subset=["skills"])

        # 1) Skills
        skills_unique = (
            exploded["skills"].drop_duplicates().reset_index(drop=True).to_frame(name="SkillName")
        )

        # 2) Jobs
        job_cols = ["job_id", "title", "url", "created", "keyword"]
        existing = [c for c in job_cols if c in df.columns]
        jobs = df[existing].drop_duplicates().copy()
        jobs_table = jobs.rename(
            columns={
                "job_id": "JobId",
                "title": "Title",
                "url": "JobLink",
                "created": "JobPosted",
                "keyword": "Keyword",
            }
        )

        # 3) JobSkills (by name for now)
        job_skills_name_only = (
            exploded[["job_id", "skills"]]
            .rename(columns={"job_id": "JobId", "skills": "SkillName"})
            .drop_duplicates()
        )

        return skills_unique, jobs_table, job_skills_name_only

    def insert_into_supabase(
        self,
        skills_unique: pd.DataFrame,
        jobs_table: pd.DataFrame,
        job_skills_name_only: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Inserts/Upserts into Supabase:
          - skills (SkillName -> SkillId)
          - jobs (JobId unique)
          - job_skills (JobId, SkillId unique)
        Returns (skills_map_df, jobs_table_final, job_skills_full).
        """
        # 1) Skills
        try:
            self._safe_upsert(
                table=self.table_skills,
                df=skills_unique,
                on_conflict="SkillName",
                batch_size=500,
            )
        except APIError as e:
            if "42P10" in str(e):
                print("[warn] skills: no UNIQUE(SkillName); falling back to manual dedupe insert.")
                self._print_constraint_guidance()
                self._manual_insert_if_no_unique(
                    table=self.table_skills, df=skills_unique, key_col="SkillName"
                )
            else:
                raise

        # Map SkillName -> SkillId
        resp = self.sb.table(self.table_skills).select("SkillId, SkillName").execute()
        skills_map_df = pd.DataFrame(resp.data or [])
        if skills_map_df.empty:
            raise RuntimeError(
                "No skills returned from Supabase; check table/permissions/RLS."
            )

        # 2) Jobs
        try:
            jobs_table_final = self._safe_upsert(
                table=self.table_jobs,
                df=jobs_table,
                on_conflict="JobId",
                batch_size=500,
            )
        except APIError as e:
            if "42P10" in str(e):
                print("[warn] jobs: no UNIQUE(JobId). Upsert requires a unique or primary key on JobId.")
                self._print_constraint_guidance()
                raise
            else:
                raise

        # 3) JobSkills (with real SkillIds)
        job_skills_full = (
            job_skills_name_only
            .merge(skills_map_df, how="inner", on="SkillName")[["JobId", "SkillId"]]
            .drop_duplicates()
        )

        try:
            for batch in self._chunked(job_skills_full.to_dict(orient="records"), size=1000):
                self.sb.table(self.table_job_skills).upsert(
                    batch, on_conflict="JobId,SkillId"
                ).execute()
        except APIError as e:
            if "42P10" in str(e):
                print("[warn] job_skills: no UNIQUE(JobId,SkillId); falling back to manual dedupe insert.")
                self._print_constraint_guidance()

                # Pull existing pairs
                resp = self.sb.table(self.table_job_skills).select("JobId, SkillId").execute()
                existing_df = pd.DataFrame(resp.data or [])
                existing_pairs = set(
                    map(tuple, existing_df[["JobId", "SkillId"]].dropna().itertuples(index=False, name=None))
                ) if not existing_df.empty else set()

                # Insert only fresh pairs
                fresh = job_skills_full[
                    ~job_skills_full.apply(lambda r: (r["JobId"], r["SkillId"]) in existing_pairs, axis=1)
                ]
                if not fresh.empty:
                    for batch in self._chunked(fresh.to_dict(orient="records"), size=1000):
                        self.sb.table(self.table_job_skills).insert(batch).execute()
            else:
                raise

        return skills_map_df, jobs_table_final, job_skills_full

    # ----------------------------
    # Internals (helpers)
    # ----------------------------
    @staticmethod
    def _to_list(x):
        if isinstance(x, list):
            return x
        if pd.isna(x):
            return []
        try:
            val = ast.literal_eval(str(x))
            if isinstance(val, list):
                return val
            return [val]
        except (ValueError, SyntaxError):
            if isinstance(x, str):
                return [s.strip() for s in x.split(",") if s.strip()]
            return []

    @staticmethod
    def _chunked(records: Iterable, size: int = 500):
        records = list(records)
        for i in range(0, len(records), size):
            yield records[i:i + size]

    def _drop_missing_column_from_payload(self, df: pd.DataFrame, message: str) -> pd.DataFrame:
        """
        Handles PGRST204 messages like:
        "Could not find the 'Keyword' column of 'jobs' in the schema cache"
        Removes that column from the payload if present and returns a new DataFrame.
        """
        parts = message.split("'")
        missing = parts[1] if len(parts) >= 2 else None
        if missing and missing in df.columns:
            print(f"[warn] Dropping missing column from payload: {missing}")
            return df.drop(columns=[missing])
        return df

    def _safe_upsert(self, *, table: str, df: pd.DataFrame, on_conflict: str, batch_size: int = 500) -> pd.DataFrame:
        """
        Upserts df into table with on_conflict.
        If PGRST204 missing-column error occurs, drops the column and retries.
        Converts datetime64 -> str to avoid PostgREST type issues.
        Returns the final (possibly column-reduced) DataFrame that succeeded.
        """
        work = df.copy()
        for col in work.columns:
            if pd.api.types.is_datetime64_any_dtype(work[col]):
                work[col] = work[col].astype(str)

        while True:
            try:
                for batch in self._chunked(work.to_dict(orient="records"), size=batch_size):
                    self.sb.table(table).upsert(batch, on_conflict=on_conflict).execute()
                return work
            except APIError as e:
                msg = str(e)
                if "PGRST204" in msg and "Could not find the" in msg:
                    new_work = self._drop_missing_column_from_payload(work, msg)
                    if new_work.shape[1] == work.shape[1]:
                        raise
                    work = new_work
                    continue
                else:
                    raise

    def _manual_insert_if_no_unique(self, *, table: str, df: pd.DataFrame, key_col: str):
        """
        Manual dedupe insert when upsert can't be used (no UNIQUE/PK on conflict target).
        key_col: the column to dedupe on (e.g., "SkillName")
        """
        resp = self.sb.table(table).select(key_col).execute()
        existing_df = pd.DataFrame(resp.data or [])
        existing_set = set(existing_df[key_col].dropna().tolist()) if not existing_df.empty else set()

        to_insert = df[~df[key_col].isin(existing_set)]
        if to_insert.empty:
            return
        for batch in self._chunked(to_insert.to_dict(orient="records"), size=500):
            self.sb.table(table).insert(batch).execute()

    def _print_constraint_guidance(self):
        """
        Prints recommended UNIQUE constraints (run once in SQL editor).
        """
        print(
            "\n[info] Recommended constraints to enable real UPSERTs (run once in SQL):\n"
            f"  -- Ensure unique skill names\n"
            f"  ALTER TABLE {self.table_skills}\n"
            f"    ADD CONSTRAINT {self.table_skills}_skillname_key UNIQUE (\"SkillName\");\n\n"
            f"  -- Ensure JobId is unique (or primary key) for jobs\n"
            f"  ALTER TABLE {self.table_jobs}\n"
            f"    ADD CONSTRAINT {self.table_jobs}_jobid_key UNIQUE (\"JobId\");\n\n"
            f"  -- Ensure no duplicate JobId+SkillId pairs\n"
            f"  ALTER TABLE {self.table_job_skills}\n"
            f"    ADD CONSTRAINT {self.table_job_skills}_jobid_skillid_key UNIQUE (\"JobId\",\"SkillId\");\n"
        )

