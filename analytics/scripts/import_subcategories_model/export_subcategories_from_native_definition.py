import os
import pandas as pd
from api.src.pcapi.core.categories.subcategories import *
from api.src.pcapi.core.categories.subcategories import ALL_SUBCATEGORIES

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "default")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
ALL_SUBCAT_DICT = []
for subcats in ALL_SUBCATEGORIES:
    ALL_SUBCAT_DICT.append(subcats.__dict__)
df_subcat = pd.DataFrame(ALL_SUBCAT_DICT)
df_subcat.to_csv("./temp.csv")
df_subcatbis = pd.read_csv("./temp.csv", index_col=[0])
df_subcatbis.to_gbq(
    f"""clean_{ENV_SHORT_NAME}.subcategories_model """,
    project_id=GCP_PROJECT_ID,
    if_exists="replace",
)
print("import DONE")
