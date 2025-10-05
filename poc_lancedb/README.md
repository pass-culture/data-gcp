# poc_lancedb

This directory contains a proof-of-concept (POC) for using LanceDB with a chatbot and a Streamlit application.

## Getting Started

1. **Run the Chatbot Data Loader**

  Before starting the Streamlit app, you **must** run the `poc_chatbot_lancedb` script. This script prepares and loads the necessary data into LanceDB.

  ```bash
  python poc_chatbot_lancedb.py
  ```

2. **Start the Streamlit App**

  Once the data is loaded, you can launch the Streamlit app:

  ```bash
  streamlit run app.py
  ```

## Directory Structure

```
poc_lancedb/
├── poc_chatbot_lancedb.py
├── app.py
└── README.md
```

## Notes

- Ensure all dependencies are installed as specified in your requirements.
- Running the chatbot loader script is required **every time** the data changes.
