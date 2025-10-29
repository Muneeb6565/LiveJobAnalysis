import os
import matplotlib
os.environ["MPLCONFIGDIR"] = "/tmp/matplotlib"   # prevents caching errors
matplotlib.use("Agg")                            # prevents font building
