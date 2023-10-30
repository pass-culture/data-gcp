from google.cloud import bigquery
import os
import pandas
import pandas_gbq
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from json import loads, dumps
from constants import BIGQUERY_ANALYTICS_DATASET,GCP_PROJECT_ID




def get_offers(number_offers=100,analytics_dataset = BIGQUERY_ANALYTICS_DATASET,gcp_project = GCP_PROJECT_ID):
    QUERY = (
            f"""SELECT 
            eim.offer_name,
            eim.offer_description,
            eim.offer_type_domain,
            eim.offer_type_labels,
            eim.offer_sub_type_label ,
            eim.author,
            eim.performer,
            rir.search_group_name,
            rir.is_numerical,
            rir.offer_is_duo,
            rir.offer_type_label,
            rir.subcategory_id, 
            rir.is_underage_recommendable,
            eim.image_url,
            rir.booking_number_last_28_days
            FROM `{gcp_project}.{analytics_dataset}.recommendable_items_raw` rir 
            left join `{gcp_project}.{analytics_dataset}.enriched_item_metadata` eim using (item_id)
            where rir.subcategory_id not like "CINE_VENTE_DISTANCE"
            order by booking_number_last_28_days desc 
            
            LIMIT {number_offers}""")
    df = pandas_gbq.read_gbq(QUERY)
    df.drop(columns=['image_url','booking_number_last_28_days'],inplace=True)
    result = df.to_json(orient="records")
    parsed = loads(result)
    return parsed
