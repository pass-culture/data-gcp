import json

import pandas as pd
import polars as pl
import requests
import streamlit as st

ENVIRONMENT = "ehp"
ENV_SHORT_NAME = "stg"
PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{ENV_SHORT_NAME}"

st.set_page_config(layout="wide")
st.title("🔍 Semantic Search API Tester")


@st.cache_data
def load_parquet():
    print("Loading Parquet data...")
    print(f"{PARQUET_FILE}/data-sorted.parquet")
    lf = pl.scan_parquet(f"{PARQUET_FILE}/data-sorted.parquet")
    return lf


lf = load_parquet()

# Available columns and operators from the API schema
AVAILABLE_COLUMNS = [
    "offer_category_id",
    "offer_subcategory_id",
    "venue_department_code",
    "last_stock_price",
    "offer_creation_date",
    "stock_beginning_date",
]

AVAILABLE_OPERATORS = ["=", ">", "<", "in", "not in", "=<", ">="]

# Initialize session state for filters
if "filters" not in st.session_state:
    st.session_state.filters = []

# ============= API CONFIGURATION =============
st.header("⚙️ API Configuration")
col1, col2 = st.columns([3, 1])

with col1:
    base_url = st.text_input(
        "Base API URL",
        value="http://localhost:8085",
        help="Base URL of the API (without the endpoint path)",
    )

with col2:
    st.write("")
    st.write("")
    if st.button("🔥 Warmup API", use_container_width=True, type="secondary"):
        try:
            warmup_response = requests.get(
                f"{base_url}/warmup",
                timeout=300
            )
            if warmup_response.ok:
                st.success("✅ API warmed up successfully!")
            else:
                st.error(f"❌ Warmup failed: {warmup_response.status_code}")
        except Exception as e:
            st.error(f"❌ Warmup error: {e}")

st.divider()

# ============= QUERY CONFIGURATION =============
st.header("🔎 Query Configuration")

col1, col2 = st.columns([2, 1])

with col1:
    search_query = st.text_input(
        "Search Query",
        value="demon slayer",
        help="Enter your search query here",
        placeholder="Type your search query..."
    )

with col2:
    st.write("")
    st.write("")
    use_filters = st.checkbox("Enable Filters", value=False)

st.divider()

# ============= FILTERS CONFIGURATION =============
if use_filters:
    st.header("🎛️ Filters Configuration")
    
    col1, col2, col3 = st.columns([1, 4, 1])
    
    with col1:
        if st.button("➕ Add Filter", use_container_width=True, type="primary"):
            st.session_state.filters.append({"column": AVAILABLE_COLUMNS[0], "operator": "=", "value": ""})
            st.rerun()
    
    with col2:
        st.write(f"**{len(st.session_state.filters)} filter(s) configured**")
    
    with col3:
        if len(st.session_state.filters) > 0:
            if st.button("🗑️ Clear All", use_container_width=True, type="secondary"):
                st.session_state.filters = []
                st.rerun()
    
    if len(st.session_state.filters) > 0:
        st.write("")
        
        # Display filters in a nice table-like layout
        filters_to_remove = []
        
        for i, filter_item in enumerate(st.session_state.filters):
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 2, 4, 1])
                
                with col1:
                    filter_item["column"] = st.selectbox(
                        "Column",
                        options=AVAILABLE_COLUMNS,
                        key=f"column_{i}",
                        index=AVAILABLE_COLUMNS.index(filter_item["column"]) if filter_item["column"] in AVAILABLE_COLUMNS else 0,
                        label_visibility="collapsed" if i > 0 else "visible"
                    )
                
                with col2:
                    filter_item["operator"] = st.selectbox(
                        "Operator",
                        options=AVAILABLE_OPERATORS,
                        key=f"operator_{i}",
                        index=AVAILABLE_OPERATORS.index(filter_item["operator"]) if filter_item["operator"] in AVAILABLE_OPERATORS else 0,
                        label_visibility="collapsed" if i > 0 else "visible"
                    )
                
                with col3:
                    # Handle different input types based on operator
                    if filter_item["operator"] in ["in", "not in"]:
                        value_input = st.text_input(
                            "Value (comma-separated)",
                            value=",".join(map(str, filter_item["value"])) if isinstance(filter_item["value"], list) else str(filter_item["value"]),
                            key=f"value_{i}",
                            help="Enter multiple values separated by commas (e.g., 1,2,3)",
                            placeholder="e.g., 1,2,3",
                            label_visibility="collapsed" if i > 0 else "visible"
                        )
                        # Parse comma-separated values
                        if value_input:
                            values = [v.strip() for v in value_input.split(",")]
                            try:
                                filter_item["value"] = [int(v) if v.isdigit() else float(v) if v.replace(".", "").isdigit() else v for v in values]
                            except:
                                filter_item["value"] = values
                        else:
                            filter_item["value"] = []
                    else:
                        value_input = st.text_input(
                            "Value",
                            value=str(filter_item["value"]) if filter_item["value"] else "",
                            key=f"value_{i}",
                            placeholder="Enter value...",
                            label_visibility="collapsed" if i > 0 else "visible"
                        )
                        # Try to convert to appropriate type
                        if value_input:
                            try:
                                if value_input.isdigit():
                                    filter_item["value"] = int(value_input)
                                elif value_input.replace(".", "").replace("-", "").isdigit():
                                    filter_item["value"] = float(value_input)
                                else:
                                    filter_item["value"] = value_input
                            except:
                                filter_item["value"] = value_input
                        else:
                            filter_item["value"] = ""
                
                with col4:
                    st.write("" if i > 0 else "Remove")
                    if st.button("🗑️", key=f"remove_{i}", help="Remove this filter"):
                        filters_to_remove.append(i)
        
        # Remove filters marked for deletion
        for i in sorted(filters_to_remove, reverse=True):
            st.session_state.filters.pop(i)
            st.rerun()
    
    st.divider()

# ============= PAYLOAD PREVIEW =============
# Build payload
payload = {
    "instances": [
        {
            "search_query": search_query
        }
    ]
}

# Add filters to payload if any exist and filters are enabled
if use_filters and st.session_state.filters:
    valid_filters = [
        f for f in st.session_state.filters 
        if f["column"] and f["operator"] and f["value"] != ""
    ]
    if valid_filters:
        payload["instances"][0]["filters_list"] = valid_filters

    # Display payload in an expander
    with st.expander("📄 View/Edit Request Payload", expanded=False):
        payload_text = st.text_area(
            "JSON Payload",
            value=json.dumps(payload, indent=2),
            height=300,
            help="You can manually edit the payload here if needed",
            label_visibility="collapsed"
        )
else:
    payload_text = json.dumps(payload, indent=2)

# ============= SEND REQUEST =============
st.write("")
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    send_button = st.button("🚀 Send Predict Request", use_container_width=True, type="primary")

if send_button:
    with st.spinner("Sending request..."):
        try:
            response = requests.post(
                f"{base_url}/predict",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                data=payload_text,
                timeout=3000,
            )
            
            st.divider()
            st.header("📊 Results")
            
            if response.ok:
                data = response.json()
                
                # Display response info
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Status Code", response.status_code)
                with col2:
                    st.metric("Response Time", f"{response.elapsed.total_seconds():.2f}s")
                with col3:
                    predictions = data.get("predictions", {})
                    offers = predictions.get("offers", [])
                    st.metric("Offers Found", len(offers) if isinstance(offers, list) else 0)
                
                st.write("")
                
                # Display offers if available
                if offers and isinstance(offers, list):
                    st.subheader("🎯 Offers")
                    st.dataframe(pd.DataFrame(offers).head(100), use_container_width=True)
                else:
                    st.info("No offers found in the response.")
                
                # Raw response in expander
                with st.expander("🔍 View Raw JSON Response"):
                    st.json(data)
            else:
                st.error(f"❌ Request failed with status code: {response.status_code}")
                st.code(response.text)
                
        except Exception as e:
            st.error(f"❌ Request failed: {e}")
else:
    st.info("👆 Configure your query and click 'Send Predict Request' to test the API")
