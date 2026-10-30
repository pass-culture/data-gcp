**embedding_offer_name**: Embedding generated with the offer name.

**embedding_offer_description**: Embedding generated with the offer description.

**embedding_offer_semantic_content**: Embedding generated with the different text attributes of the offer.

**embedding_offer_image**: Embedding generated with the offer image.

**embedding_offer_hybrid**: Embedding generated with the image and the semantic content of an image.

**two_tower_item_embedding**: Two tower model item embedding.

**two_tower_user_embedding**: Two tower model user embedding.

**content_hash**: Hash of item's metadata.

**semantic_content**: Embedding of item's metadata with "document" prompt name. Encoded with embedding-gemma-300m model.

**semantic_content_128**: Truncated embedding of item's metadata with "document" prompt name of size 128. Encoded with embedding-gemma-300m model. Truncated with Matryoshka Representation Learning. NOT NORMALIZED!

**all_items_metadata_embedding**: Embedding of the item's metadata.

**all_items_metadata_embedding_128**: First 128 dimensions of the item's metadata embedding, truncated with Matryoshka Representation Learning (MRL). NOT NORMALIZED!

**movies_metadata_embedding**: Embedding of the movie item's metadata.

**books_metadata_embedding**: Embedding of the book item's metadata.

**all_items_offer_names_embedding**: Embedding of the offer name associated with the item.

**all_items_offer_names_embedding_128**: First 128 dimensions of the item's offer name embedding, truncated with Matryoshka Representation Learning (MRL). NOT NORMALIZED!

**embedding_model**: Name of the model used to generate the embedding.

**embedding_date**: Date at which the embedding was generated.
