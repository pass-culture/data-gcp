import pandas as pd
from api.src.pcapi.core.categories.subcategories import *

ALL_SUBCAT_DICT = {}
for subcats in ALL_SUBCATEGORIES:
    ALL_SUBCAT_DICT[f"{subcats.id}"] = subcats.__dict__
df_subcat = pd.DataFrame(ALL_SUBCAT_DICT)
print(df_subcat.head())
