"""Home page of the Streamlit app."""

import streamlit as st

def main() -> None:
	"""Home page of the Streamlit app."""
	st.set_page_config(layout="wide")
	st.title("Data-GCP RAG Suite")

	app_choice = st.sidebar.radio(
		"Choose an app:",
		["Home", "RAG Chatbot", "RAG vs Labellised Evaluation"]
	)

	if app_choice == "Home":
		st.header("Welcome to the Data-GCP RAG Suite")
		st.write("Select an app from the sidebar to get started.")

	elif app_choice == "RAG Chatbot":
		# Import and run your chatbot logic here
		import app
		app.run()

	elif app_choice == "RAG vs Labellised Evaluation":
		# Import and run your evaluation logic here
		import compare_rag_vs_labellised_app
		compare_rag_vs_labellised_app.run()


if __name__ == "__main__":
	main()
