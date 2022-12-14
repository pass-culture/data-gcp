from tools.config import STORAGE_PATH
import pandas as pd 

def preprocess(df):
    df["linked_id"]=['NC']*len(df)
    df['offer_id'] = df['offer_id'].values.astype(int)
    df['offer_name'] = df['offer_name'].str.lower()
    df['offer_description'] = df['offer_description'].str.lower()
    return df
    
def main() -> None:
    df_offers_to_link=pd.read_csv(f"{STORAGE_PATH}/offers_to_link.csv")
    df_offers_to_link_clean=preprocess(df_offers_to_link)
    df_offers_to_link_clean.to_csv(f"{STORAGE_PATH}/offers_to_link_clean.csv")
if __name__ == "__main__":
    main()
