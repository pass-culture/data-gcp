# Linkage


📂 Structure of Offers in the Pass Culture Database

Product 🎥: This groups together all the offers related to the same cultural 'object'. For example, all the screenings of the same film offered in France.

Offer 📍: This is a specific reference to this cultural object, offered in a well-defined location.
🔑 Product Creation

Products are created from identifiers (like EAN or Visa) provided by the cultural actor. These identifiers allow us to query external APIs to generate metadata for the offer (name, description, genres, etc.).

The goal is to link Offers that are not yet associated with a Product, but should be, with the specific objective of enhancing the user experience and thereby improving the discoverability of the catalog

🔍 Matching Offers and Products

To solve this problem of matching Offers and Products, tools like the ones presented below can be used. These scripts allow us to perform similarity searches, compare data, and automate the matching between unrelated records. For example, one script generates semantic candidates from offer metadata and compares them to existing products to identify potential matches. These approaches optimize the matching process by analyzing names, descriptions, and other textual information, while efficiently managing large volumes of data through parallelization.

In simpler terms:

Imagine a database for the "Pass Culture" program (a French cultural initiative). This database has:

Products: Think of a movie. A "Product" is like the general concept of that movie.
Offers: These are the specific screenings of that movie, with details like location, time, and date.
Sometimes, new offers are added to the database, but they aren't automatically linked to the correct "Product".  The goal is to find those unlinked offers and connect them to the right "Product" (like connecting a movie screening to the general information about that movie).

They use tools and scripts to do this by:

Looking for similarities: Comparing the offer's information (name, description) to existing products to find potential matches.
Using external data: Getting more information about the offer from other sources (like APIs) to help with identification.
This helps them keep the database organized and make sure all offers are correctly linked to their corresponding products.

⚙️ Operation

🛠️ Preparing Data for Ingestion into LanceDB

This Python script aims to prepare embedding data (vectors representing items) and store it in a LanceDB vector database. It also performs dimensionality reduction of the embeddings if necessary, and then creates a table and an index to facilitate access and search within the embedding vectors. This process involves several steps: loading data from Parquet files stored on Google Cloud Storage (GCS), preprocessing embeddings, creating a database in LanceDB, and indexing for optimized searches.

🧠 Candidate Generation

This Python script preprocesses embedding data, generates semantic candidates based on embedding similarity, and stores the results as Parquet files in Google Cloud Storage (GCS). The script uses a dimensionality reduction model called HNNE (Hierarchical Nearest Neighbor Embedding) to reduce embeddings, and the SemanticSpace model to perform vector similarity searches between items.

🔗 Record Linkage (matching by comparing two datasets)

This Python script performs record linkage by comparing two datasets. It uses string comparison methods (offer name, artist/performer, etc.) to identify similar record pairs between two datasets. The process involves several steps: data cleaning, index creation to generate candidates to compare, record comparison via a matching model, and storing results as a Parquet file in Google Cloud Storage (GCS).
