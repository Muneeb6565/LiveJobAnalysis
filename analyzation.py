import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import io
import base64
from collections import Counter

import numpy as np
import pandas as pd

from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
import pandas as pd
from check import SkillsTrendAdapter


class AnalyzationPipeline:
    def __init__(self):
        self.response = None
        self.df = None  # stored for skill_trends()

    # ---------- public API ----------
    def analyze_top_skills(
        self,
        df: pd.DataFrame,
        analyze: bool = True,     # FLAG: True = cluster + analyze; False = just plot counts
        top_k: int = 8,
        min_count: int = 2        # ignore singletons in either mode
    ) -> str:
        """
        Returns: base64 PNG of the bar chart ('' if nothing to plot)

        analyze=True  -> semantic clustering into categories, then plot categories
        analyze=False -> plain top skill frequencies (no embeddings/clustering)
        """
        if not isinstance(df, pd.DataFrame):
            print("❌ Input must be a DataFrame.")
            return ""

        if "skills" not in df.columns or df["skills"].dropna().empty:
            print("❌ No 'skills' column or it is empty.")
            return ""

        # keep a copy for skill_trends() usage
        self.df = df.copy()
        # safely drop large text column if present
        self.df = self.df.drop(columns=[c for c in ["description", "descripiton"] if c in self.df.columns], errors="ignore")

        # normalize to one skill per row
        skills_series = self._explode_skills(self.df["skills"])
        if skills_series.empty:
            print("❌ No skills after normalization.")
            return ""

        # filter tiny noise
        counts = Counter(skills_series)
        counts = {s: c for s, c in counts.items() if c >= min_count}
        if not counts:
            print("❌ No skills pass the min_count filter.")
            return ""

        if analyze:
            # -------- semantic clustering path --------
            unique_skills = list(counts.keys())
            model = SentenceTransformer("all-MiniLM-L6-v2")
            emb = model.encode(unique_skills, show_progress_bar=False, normalize_embeddings=True)

            clustering = AgglomerativeClustering(
                linkage="average",
                metric="cosine",
                distance_threshold=0.35,  # ~0.65 cosine similarity
                n_clusters=None
            )
            labels = clustering.fit_predict(np.array(emb))

            # build clusters
            cluster_map = {}
            for label, skill in zip(labels, unique_skills):
                cluster_map.setdefault(label, []).append((skill, counts[skill]))

            # turn clusters into category entries
            categories = []
            for _, skill_list in cluster_map.items():
                skill_list.sort(key=lambda x: x[1], reverse=True)
                total = sum(c for _, c in skill_list)
                main = skill_list[0][0].title()
                ex = [s.title() for s, _ in skill_list[:4]]
                categories.append({
                    "category": main,
                    "total_jobs": total,
                    "examples": ex,
                    "num_skills": len(skill_list)
                })

            top = sorted(categories, key=lambda x: x["total_jobs"], reverse=True)[:top_k]
            return self._plot_categories(top)

        else:
            # -------- simple frequency path (no embeddings) --------
            series = pd.Series(counts).sort_values(ascending=False).head(top_k)
            if series.empty:
                return ""
            names = [s.title() for s in series.index.tolist()]
            vals = series.values.tolist()
            return self._plot_bars(names, vals)

    def skill_trends(self):
        """Unchanged: uses self.df set in analyze_top_skills()."""
        if self.df is None:
            print("❌ Run analyze_top_skills() first (to set self.df).")
            return "", []
        adapter = SkillsTrendAdapter(dataframe=self.df)
        # self.df.to_csv('1000_rows.csv', index=False)
        img_b64, ranked_skills = adapter.skill_trends()
        return img_b64, ranked_skills

    # ---------- helpers ----------
    @staticmethod
    def _explode_skills(s: pd.Series) -> pd.Series:
        """
        Accepts comma-separated strings or python lists and returns
        a clean, exploded, lower-cased Series of skills.
        """
        # if any rows are lists, explode directly
        if s.apply(lambda x: isinstance(x, list)).any():
            tmp = pd.DataFrame({"skills": s}).explode("skills")["skills"]
        else:
            tmp = s.dropna().astype(str).str.split(",").explode()

        tmp = tmp.astype(str).str.strip().str.lower()
        return tmp[tmp != ""]

    @staticmethod
    def _plot_bars(names, counts) -> str:
        """Simple barh plot for top skills (no examples)."""
        fig, ax = plt.subplots(figsize=(12, 8))
        fig.patch.set_facecolor("#f8f9fa")

        palette = [
            "#3498db", "#e74c3c", "#2ecc71", "#f39c12",
            "#9b59b6", "#1abc9c", "#34495e", "#e67e22"
        ]
        bars = ax.barh(range(len(names)), counts, color=palette[:len(names)], alpha=0.85)

        for bar, val in zip(bars, counts):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{val:,}", va="center", fontsize=10, color="#2c3e50")

        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(names, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel("Mentions", fontsize=12)
        ax.set_title("Top Skills (Frequency Only)", fontsize=14, pad=10)
        ax.grid(axis="x", linestyle="--", alpha=0.3)
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode("utf-8")
        #plt.show()
        plt.close(fig)
        return b64

    @staticmethod
    def _plot_categories(top_categories: list) -> str:
        """Category barh plot with examples (semantic clustering path)."""
        if not top_categories:
            return ""

        names = [c["category"] for c in top_categories]
        counts = [c["total_jobs"] for c in top_categories]

        fig, ax = plt.subplots(figsize=(12, 8))
        fig.patch.set_facecolor("#f8f9fa")

        palette = [
            "#3498db", "#e74c3c", "#2ecc71", "#f39c12",
            "#9b59b6", "#1abc9c", "#34495e", "#e67e22"
        ]
        bars = ax.barh(range(len(names)), counts, color=palette[:len(names)], alpha=0.85)

        for bar, val in zip(bars, counts):
            ax.text(bar.get_width() + 2, bar.get_y() + bar.get_height() / 2,
                    f"{val:,} jobs", va="center", fontsize=10, color="#2c3e50")

        # label with examples
        y_labels = []
        for c in top_categories:
            ex = ", ".join(c["examples"][:3])
            if len(c["examples"]) > 3:
                ex += f" +{c['num_skills'] - 3} more"
            y_labels.append(f"{c['category']}\n({ex})")

        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(y_labels, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel("Number of Job Opportunities", fontsize=12)
        ax.set_title("Top Skill Categories (Semantic Clusters)", fontsize=14, pad=10)
        ax.grid(axis="x", linestyle="--", alpha=0.3)
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
        buf.seek(0)
        b64 = base64.b64encode(buf.read()).decode("utf-8")
        #plt.show()
        plt.close(fig)
        return b64


# quick example usage
# if __name__ == "__main__":
#     # df = pd.read_csv("merged.csv")

#     pipe = AnalyzationPipeline()

    # 1) Full analysis + clustered categories plot
    # plot_b64_analysis = pipe.analyze_top_skills(df, analyze=True)

#     # 2) Only frequency plot (no embeddings/clustering)
#     plot_b64_simple = pipe.analyze_top_skills(df, analyze=False)

#     # 3) Trend plot (uses df stored from last call)
#     trend_b64, ranked = pipe.skill_trends()
#     print("Trend skills (top):", ranked[:10])
#     print(trend_b64)























