import streamlit as st

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")


st.title("🧪 Create Test Set for Artist Linkage")
st.markdown(
    "### This app allows you to create different test sets (slices) for artist linkage"
)
st.markdown("---")


st.markdown("##### To do so:")
st.page_link(
    "pages/1_🧬_Generate_data_to_labelize.py",
    label="Generate the data you want to labelize 🧬",
    icon="1️⃣",
)
st.page_link(
    "pages/2_🏷️_Labelize_test_sets.py",
    label="Labellize the data 🏷️",
    icon="2️⃣",
)
st.page_link(
    "pages/3_🔃_Review_test_sets.py",
    label="Review the labelized test sets 🔃",
    icon="3️⃣",
)
