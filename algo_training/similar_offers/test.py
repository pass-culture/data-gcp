import requests
import json
from custom_prediction_VertexAI import predict_custom_trained_model_sample

# r = requests.get("http://localhost:8080/isalive")
# print(r)
"""

r = requests.post("http://localhost:8080/predict", json={"instances":[{"offer_id": "product-2951941"}]})
print(r)
#<Response [200]>
r_json = r.json()
print(r_json)
#<Response [200]>

"""
resp = predict_custom_trained_model_sample(
    project="815655901630",
    endpoint_id="6457194295416848384",
    location="europe-west1",
    instances={"offer_id": "product-2951941"},
)
print(resp)
