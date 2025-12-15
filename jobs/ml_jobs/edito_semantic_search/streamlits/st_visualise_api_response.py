import streamlit as st
import requests
import pandas as pd
import json

st.set_page_config(layout="wide")
st.title("Visualize /predict API Response")

st.sidebar.header("Request Parameters")

# Default payload
payload = {
    "instances": [
        {
            "search_query": "demon slayer"
        }
    ]
}

api_url = st.sidebar.text_input(
    "API URL", value="http://localhost:8082/predict", help="URL of the /predict endpoint"
)
payload_text = st.sidebar.text_area(
    "Request JSON payload", value=json.dumps(payload, indent=2), height=200
)

if st.sidebar.button("Send Request"):
    try:
        response = requests.post(
            api_url,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=payload_text,
            timeout=3000,
        )
        st.subheader("Raw Response")
        st.code(response.text, language="json")
        if response.ok:
            data = response.json()
            # Try to find offers or results in the response
            offers = data.get("offers") or data.get("results") or data.get("items")
            if offers and isinstance(offers, list):
                st.subheader("Offers Table")
                st.dataframe(pd.DataFrame(offers))
            else:
                st.info("No offers/results/items found in response.")
        else:
            st.error(f"Error: {response.status_code} {response.reason}")
    except Exception as e:
        st.error(f"Request failed: {e}")
else:
    st.info("Set parameters and click 'Send Request' to visualize the response.")
