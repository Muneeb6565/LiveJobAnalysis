import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64
import numpy as np
import pandas as pd



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

