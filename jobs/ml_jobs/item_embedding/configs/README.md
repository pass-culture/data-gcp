# Configuration for item embedding vectors
Each yaml defines an embedding vector and which features to include in the embedding prompt and the model to use for embedding extraction

Features will be concatenated in the order specified for each vector type
Feature may have an optional label override in the "labels" section, which will be used in the prompt instead of the column name
e.g. the "semantic_content" vector will encode:
    titre de l'offre : <offer_name_value>
    catégorie : <category_id_value>
into a single prompt for embedding extraction

# Available prompt_names for embeddinggemma:
['query', 'document', 'BitextMining', 'Clustering', 'Classification',
'InstructionRetrieval', 'MultilabelClassification', 'PairClassification',
'Reranking', 'Retrieval', 'Retrieval-query', 'Retrieval-document', 'STS', 'Summarization'].
(see https://huggingface.co/google/embeddinggemma-300m)

We generally use:
"document" prompt: "title: none | text: {content}"
