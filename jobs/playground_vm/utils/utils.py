from google.cloud import bigquery
import pandas
import pandas_gbq
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from json import loads, dumps


ENV_SHORT_NAME ='prod'

BIGQUERY_ANALYTICS_DATASET='analytics_prod'
GCP_PROJECT_ID='passculture-data-prod'


def get_offers(number_offers=100,ENV_SHORT_NAME ='prod',BIGQUERY_ANALYTICS_DATASET='analytics_prod',GCP_PROJECT_ID='passculture-data-prod'):
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
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.recommendable_items_raw` rir 
            left join `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.enriched_item_metadata` eim using (item_id)
            where rir.subcategory_id not like "CINE_VENTE_DISTANCE"
            order by booking_number_last_28_days desc 
            
            LIMIT {number_offers}""")
    df = pandas_gbq.read_gbq(QUERY)
    df.drop(columns=['image_url','booking_number_last_28_days'],inplace=True)
    result = df.to_json(orient="records")
    parsed = loads(result)
    return parsed
