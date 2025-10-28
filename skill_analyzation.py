# import io
# import base64
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt

# class WilsonNecessityWidget:
#     def __init__(
#         self,
#         df: pd.DataFrame,
#         skills_col: str = "skills",
#         min_support: int = 20,
#         nec_wlb_pct: float = 40.0,  # <-- Necessary threshold @ 40%
#         nice_min_pct: float = 20.0, # Better-to-have band (lower)
#         nice_max_pct: float = 60.0, # Better-to-have band (upper)
#         collapse_ml: bool = True,
#         ml_terms: set | None = None,
#         ml_name: str = "ml (domain)",
#         drop_ml_originals: bool = True,
#     ):
#         if skills_col not in df.columns:
#             raise ValueError(f"Column '{skills_col}' not found in DataFrame.")
#         self.df = df.copy()
#         self.skills_col = skills_col

#         self.min_support = min_support
#         self.nec_wlb_pct = nec_wlb_pct
#         self.nice_min_pct = nice_min_pct
#         self.nice_max_pct = nice_max_pct

#         self.collapse_ml = collapse_ml
#         self.ml_terms = ml_terms or {
#             "ml", "machine learning", "deep learning", "ai",
#             "tensorflow", "pytorch", "scikit-learn", "scikit learn",
#             "nlp", "llms", "data science"
#         }
#         self.ml_name = ml_name
#         self.drop_ml_originals = drop_ml_originals

#         self.n_posts = len(self.df)  # total postings
#         self.data: pd.DataFrame | None = None

#     # -------- helpers --------
#     @staticmethod
#     def clean_and_split(s):
#         """Comma-separated skills -> deduped, lowercased list (per posting)."""
#         if pd.isna(s):
#             return []
#         parts = [p.strip().lower() for p in str(s).split(",")]
#         parts = [p for p in parts if p]
#         return sorted(set(parts))

#     @staticmethod
#     def wilson_lower(k: int, n: int, z: float = 1.96) -> float:
#         """95% Wilson lower confidence bound for proportion k/n."""
#         if n <= 0:
#             return 0.0
#         p = k / n
#         den = 1 + (z**2) / n
#         center = p + (z**2) / (2 * n)
#         margin = z * np.sqrt((p * (1 - p) / n) + (z**2) / (4 * n**2))
#         return (center - margin) / den

#     def _build_counts(self) -> pd.DataFrame:
#         """Build base table with counts, share, and Wilson metrics."""
#         self.df["skill_list"] = self.df[self.skills_col].apply(self.clean_and_split)
#         counts = self.df["skill_list"].explode().value_counts()

#         data = pd.DataFrame({
#             "name": counts.index,
#             "counts": counts.values.astype(int)
#         })
#         data["share"] = data["counts"] / float(self.n_posts)
#         data["percentage"] = data["share"] * 100.0
#         data["wilson_lower"] = [self.wilson_lower(int(k), self.n_posts) for k in data["counts"]]
#         data["wilson_lower_pct"] = data["wilson_lower"] * 100.0
#         return data.sort_values("share", ascending=False).reset_index(drop=True)

#     def _collapse_ml_domain(self, data: pd.DataFrame) -> pd.DataFrame:
#         """Optionally aggregate ML tokens into one 'ml (domain)' row."""
#         if not self.collapse_ml:
#             return data
#         d = data.copy()
#         ml_mask = d["name"].str.lower().isin(self.ml_terms)
#         ml_count = int(d.loc[ml_mask, "counts"].sum())
#         if ml_count == 0:
#             return d

#         ml_share = ml_count / float(self.n_posts)
#         ml_pct = ml_share * 100.0
#         ml_wlb = self.wilson_lower(ml_count, self.n_posts)
#         ml_wlb_pct = ml_wlb * 100.0

#         ml_row = pd.DataFrame([{
#             "name": self.ml_name,
#             "counts": ml_count,
#             "share": ml_share,
#             "percentage": ml_pct,
#             "wilson_lower": ml_wlb,
#             "wilson_lower_pct": ml_wlb_pct,
#         }])

#         if self.drop_ml_originals:
#             d = d.loc[~ml_mask].copy()

#         return pd.concat([d, ml_row], ignore_index=True)

#     def _label_skill(self, row) -> str:
#         if row["counts"] >= self.min_support and row["wilson_lower_pct"] >= self.nec_wlb_pct:
#             return "Necessary"
#         elif row["counts"] >= self.min_support and (self.nice_min_pct <= row["percentage"] < self.nice_max_pct):
#             return "Better-to-have"
#         else:
#             return "Other"

#     # -------- public API --------
#     def run(self) -> pd.DataFrame:
#         """Execute pipeline; returns labeled table."""
#         data = self._build_counts()
#         data = self._collapse_ml_domain(data)
#         data["label"] = data.apply(self._label_skill, axis=1)
#         self.data = data
#         return self.data

#     def plot(self, annotate_top: int = 12,
#              title: str = "Must-have vs Nice-to-have (Wilson vs Share)",
#              show_legend: bool = True):
#         """Create the scatter plot (fig, ax)."""
#         if self.data is None:
#             raise RuntimeError("Call .run() before .plot().")
#         d = self.data.copy()
#         d = d[d["counts"] > 0]

#         fig, ax = plt.subplots(figsize=(11, 7))

#         # Shaded Better-to-have band (Y) and Necessary threshold (X)
#         ax.axhspan(self.nice_min_pct, self.nice_max_pct, alpha=0.08)
#         ax.axvline(self.nec_wlb_pct, linestyle="--")

#         # Scatter by label; NO counts in legend labels
#         for label, g in d.groupby("label"):
#             sizes = np.clip(g["counts"] * 3, 30, 600)
#             ax.scatter(g["wilson_lower_pct"], g["percentage"], s=sizes, alpha=0.8, label=label)

#         ax.set_xlabel("Wilson lower bound (%) — conservative commonness")
#         ax.set_ylabel("Raw share (%) — how often the skill appears")
#         ax.set_title(title)

#         # Annotate top skills by support
#         top = d.sort_values("counts", ascending=False).head(annotate_top)
#         for _, r in top.iterrows():
#             ax.annotate(r["name"], (r["wilson_lower_pct"], r["percentage"]),
#                         xytext=(5, 5), textcoords="offset points", fontsize=9)

#         if show_legend:
#             ax.legend(frameon=False)
#         ax.grid(True, alpha=0.2)
#         plt.tight_layout()
#         return fig, ax

#     def plot_base64(self, annotate_top: int = 12,
#                     title: str = "Must-have vs Nice-to-have (Wilson vs Share)",
#                     show_legend: bool = True,
#                     dpi: int = 150) -> str:
#         """Render plot and return a data:image/png;base64,... string."""
#         fig, ax = self.plot(annotate_top=annotate_top, title=title, show_legend=show_legend)
#         buf = io.BytesIO()
#         fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
#         # #plt.show()
#         plt.close(fig)  # free memory
#         b64 = base64.b64encode(buf.getvalue()).decode("ascii")
#         return b64





import io
import base64
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


class WilsonNecessityWidget:
    def __init__(
        self,
        df: pd.DataFrame,
        skills_col: str = "skills",
        min_support: int = 20,
        nec_wlb_pct: float | None = None,   # None = auto-threshold
        nice_min_pct: float = 20.0,
        nice_max_pct: float = 60.0,
        collapse_ml: bool = True,
        ml_terms: set | None = None,
        ml_name: str = "ml (domain)",
        drop_ml_originals: bool = True,
    ):
        if skills_col not in df.columns:
            raise ValueError(f"Column '{skills_col}' not found.")

        self.df = df.copy()
        self.skills_col = skills_col

        self.min_support = min_support
        self.nec_wlb_pct = nec_wlb_pct   # <-- If None, will auto-calc later
        self.nice_min_pct = nice_min_pct
        self.nice_max_pct = nice_max_pct

        self.collapse_ml = collapse_ml
        self.drop_ml_originals = drop_ml_originals
        self.ml_name = ml_name
        self.ml_terms = ml_terms or {
            "ml", "machine learning", "deep learning", "ai",
            "tensorflow", "pytorch", "scikit-learn", "nlp",
            "llms", "data science"
        }

        self.n_posts = len(self.df)
        self.data = None

    # ---------- Core Math ----------
    @staticmethod
    def wilson_lower_vectorized(k, n, z=1.96):
        p = k / n
        z2 = z**2
        denom = 1 + z2 / n
        center = p + z2 / (2 * n)
        margin = z * np.sqrt((p * (1 - p) / n) + z2 / (4 * n**2))
        return (center - margin) / denom

    @staticmethod
    def clean_and_split(s):
        if pd.isna(s):
            return []
        return sorted(set(p.strip().lower() for p in str(s).split(",") if p.strip()))

    # ---------- Data Prep ----------
    def _build_counts(self):
        self.df["skill_list"] = self.df[self.skills_col].apply(self.clean_and_split)
        counts = self.df["skill_list"].explode().value_counts()

        data = pd.DataFrame({
            "name": counts.index,
            "counts": counts.values
        })

        data["share"] = data["counts"] / self.n_posts
        data["percentage"] = data["share"] * 100
        data["wilson_lower"] = self.wilson_lower_vectorized(data["counts"], self.n_posts)
        data["wilson_lower_pct"] = data["wilson_lower"] * 100

        return data.reset_index(drop=True)

    # ---------- ML Collapse ----------
    def _collapse_ml(self, data):
        if not self.collapse_ml:
            return data

        ml_mask = data["name"].str.lower().isin(self.ml_terms)
        ml_total = data.loc[ml_mask, "counts"].sum()

        if ml_total == 0:
            return data

        ml_row = pd.DataFrame([{
            "name": self.ml_name,
            "counts": ml_total,
            "share": ml_total / self.n_posts,
            "percentage": (ml_total / self.n_posts) * 100,
            "wilson_lower": self.wilson_lower_vectorized(ml_total, self.n_posts),
            "wilson_lower_pct": self.wilson_lower_vectorized(ml_total, self.n_posts) * 100
        }])

        if self.drop_ml_originals:
            data = data.loc[~ml_mask]

        return pd.concat([data, ml_row], ignore_index=True)

    # ---------- Labeling ----------
    def _label(self, row):
        if row["counts"] < self.min_support:
            return "Other"
        if row["wilson_lower_pct"] >= self.nec_wlb_pct:
            return "Necessary"
        if self.nice_min_pct <= row["percentage"] < self.nice_max_pct:
            return "Better-to-have"
        return "Other"

    # ---------- Public API ----------
    def run(self):
        data = self._build_counts()
        data = self._collapse_ml(data)

        # ---- Auto-set threshold if not provided ----
        if self.nec_wlb_pct is None:
            self.nec_wlb_pct = np.percentile(data["wilson_lower_pct"], 75)

        data["label"] = data.apply(self._label, axis=1)
        self.data = data
        return data.sort_values("wilson_lower_pct", ascending=False)

    # ---------- Plot ----------
    def plot(self, annotate_top=12):
        if self.data is None:
            raise RuntimeError("Call .run() first.")

        d = self.data[self.data["counts"] > 0]

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axhspan(self.nice_min_pct, self.nice_max_pct, alpha=0.08)
        ax.axvline(self.nec_wlb_pct, linestyle="--", alpha=0.7)

        for label, g in d.groupby("label"):
            ax.scatter(g["wilson_lower_pct"], g["percentage"],
                       s=np.clip(g["counts"] * 3, 30, 500),
                       alpha=0.8, label=label)

        top = d.nlargest(annotate_top, "counts")
        for _, r in top.iterrows():
            ax.annotate(r["name"], (r["wilson_lower_pct"], r["percentage"]),
                        xytext=(4, 4), textcoords="offset points", fontsize=8)

        ax.set_xlabel("Wilson Lower (%) — Conservative Commonness")
        ax.set_ylabel("Raw Share (%)")
        ax.set_title("Necessary vs Better-to-Have Skills")
        ax.legend(frameon=False)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        return fig, ax

    def plot_base64(self, dpi=150):
        fig, ax = self.plot()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        return base64.b64encode(buf.getvalue()).decode("ascii")

