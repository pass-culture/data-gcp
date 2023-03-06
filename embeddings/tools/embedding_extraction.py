import json 
import time
from sentence_transformers import SentenceTransformer
import urllib.request
from tools.logging_tools import log_duration
from PIL import Image

def extract_embedding(    
    df_data,
    params,
):
    """
    Extarct embedding with pretrained models 
    Two types available:
    - image : 
        - Input: list of urls 
    - text  : 
        - Input: list of string
    """
    start = time.time()
    
    for feature in params['features']:
        if feature["type"]=="image":
            model=SentenceTransformer('clip-ViT-B-32')
            urls=df_data.url
            df_data[f"{feature["name"]}_embedding"]=encode_img_from_urls(model,urls)
        if feature["type"]=="text":
            model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
            embeddings = model.encode(df_data[f"{feature["name"]}"])
            df_data[f"{feature["name"]}_embedding"]=embeddings
    log_duration(f"Embedding extraction: ", start)
    return df_data
    

def encode_img_from_urls(model,urls):
        index=0
        offer_img_embs=[]
        offer_wO_img=0
        for url in tqdm(urls):
            STORAGE_PATH_IMG=f'./img/{index}'
            _download_img_from_url(url,STORAGE_PATH_IMG)
            try :
                img_emb = model.encode(Image.open(f"{STORAGE_PATH_IMG}.jpeg"))
                offer_img_embs.append(list(img_emb))
                os.remove(f"{STORAGE_PATH_IMG}.jpeg")
                index+=1
            except: 
                offer_img_embs.append([0]*512)
                index+=1
                offer_wO_img+=1
        print(f"{(offer_wO_img*100)/len(urls)}% offers dont have image")
        return offer_img_embs

def _download_img_from_url(url,storage_path):
            try:
                urllib.request.urlretrieve(url,f"{storage_path}.jpeg")
            except:        
                return None 


