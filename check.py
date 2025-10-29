import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import io
import base64
import pandas as pd
import matplotlib.dates as mdates
import numpy as np




class SkillsTrendAdapter:
    def __init__(self, csv_path: str = None, dataframe: pd.DataFrame = None):
        """Initialize adapter, clean data, and normalize fields."""
        if dataframe is not None:
            df = dataframe.copy()
        # elif csv_path:
        #     df = pd.read_csv(csv_path)
        # else:
        #     df = pd.read_csv('1000_rows.csv')

        required = {"created", "skills"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"DataFrame must contain columns: {sorted(required)}. Missing: {sorted(missing)}"
            )

        df["created"] = self._parse_mixed_dates(df["created"]).dt.floor("D")

        df["skills"] = df["skills"].apply(self._to_list)
        df = df.explode("skills", ignore_index=True)

        df["skills"] = (
            df["skills"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df = df[df["skills"].notna() & (df["skills"] != "")]
        df = df[df["created"].notna()]

        df = df.rename(columns={"created": "JobPosted", "skills": "SkillName"})
        self.df = df

    @staticmethod
    def _to_list(x):
        """Ensure values are lists for explosion."""
        if pd.isna(x):
            return []
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            parts = [p.strip() for p in x.replace(";", ",").split(",") if p.strip()]
            return parts
        return [x]

    @staticmethod
    def _parse_mixed_dates(s: pd.Series) -> pd.Series:
        """Parse multiple date formats robustly."""
        s = (
            s.astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "NaT": pd.NA})
        )
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        iso = s.str.match(r"^\d{4}-\d{2}-\d{2}T").fillna(False)
        rfc = s.str.match(r"^[A-Za-z]{3},\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}").fillna(False)
        ymd = s.str.match(r"^\d{4}-\d{2}-\d{2}$").fillna(False)
        slash = s.str.match(
            r"^\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2})?$"
        ).fillna(False)

        def to_naive_utc(x):
            dt = pd.to_datetime(x, errors="coerce", utc=True)
            return dt.dt.tz_localize(None)

        out.loc[iso] = to_naive_utc(s.loc[iso])
        out.loc[rfc] = to_naive_utc(s.loc[rfc])
        out.loc[ymd] = to_naive_utc(s.loc[ymd])
        out.loc[slash] = pd.to_datetime(
            s.loc[slash], errors="coerce", dayfirst=True
        )

        rem = out.isna()
        out.loc[rem] = pd.to_datetime(s.loc[rem], errors="coerce", dayfirst=True)
        rem = out.isna()
        out.loc[rem] = pd.to_datetime(s.loc[rem], errors="coerce")

        return out

    def skill_trends(self):
        """Return top 5 most frequent skills per day and plot trend."""
        df = self.df

        top_fields_all_time = df.SkillName.value_counts()[:5].index.to_list()        
        df = df.loc[df.SkillName.isin(top_fields_all_time )]

        counts = (
            df.groupby(["JobPosted", "SkillName"])
            .size()
            .reset_index(name="count")
        )

        print(counts.tail(30))

        top5_per_day = (
            counts.sort_values(["JobPosted", "count"], ascending=[True, False])
            .groupby("JobPosted", group_keys=False)
            .head(5)[["JobPosted", "SkillName", "count"]]
            .reset_index(drop=True)
        )

        fig, ax = plt.subplots(figsize=(12, 7))

        top5_per_day["JobPosted"] = pd.to_datetime(top5_per_day["JobPosted"])
        top5_per_day = top5_per_day.loc[top5_per_day.JobPosted > '2025-08-8'] 

        for skill in top5_per_day["SkillName"].unique():
            skill_data = (
                top5_per_day[top5_per_day["SkillName"] == skill]
                .sort_values("JobPosted")
            )
            ax.plot(
                skill_data["JobPosted"],
                skill_data["count"],
                marker="o",
                linewidth=2,
                label=skill,
                alpha=0.9
            )

        ax.set_title("Top 5 Skills per Day — Line Trends", fontsize=14, fontweight="bold")
        ax.set_xlabel("Date", fontsize=12, fontweight="bold")
        ax.set_ylabel("Count", fontsize=12, fontweight="bold")
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.set_facecolor("#fafafa")

        # X-axis date formatting
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        plt.xticks(rotation=45, ha="right")

        # Legend
        ax.legend(
            title="Skill",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            frameon=True
        )

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        # plt.show()
        plt.close(fig)
        
        buf.seek(0)
        png_b64 = base64.b64encode(buf.read()).decode("ascii")

        return png_b64, top_fields_all_time 



# if __name__ == "__main__":
#     adapter = SkillsTrendAdapter()
#     trends = adapter.skill_trends()
#     print("\n✅ Final Top 5 Skills per Day:")
#     print(trends)
