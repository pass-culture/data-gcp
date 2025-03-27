# The Pass Culture Recommendation Engine

Our recommendation engine aims to showcase relevant and personalized items from our extensive collection of 2 million cultural goods and events to our xM users.

Basically, we are training a model which allows to provide :
* Recommendations to a given user (we call it Home Recommendations)
* Similar offers to a given offer (we call it Similar Offers Recommendations)



# The pipeline architecture

We are using a Retrieval-Ranking model, which is a model that retrieves a list of items from a large corpus and ranks them in order of relevance to a given query, as you can see below.
![Retrieval-Ranking model](assets/retrieval-ranking-architecture.png)

Indeed, the model is composed of three parts:

* A **Retrieval model** : it retrieves a list of items from a very large corpus. Think from Millions to Hundreds.
    > The model need not to be very accurate, but it needs to be very fast and scalable.
* A **Ranking model** : it ranks the retrieved items in order of relevance to a given query. In our framework, we also use it to filter out the items that are not relevant to the user, so that we keep less than a hundred results.
    > The model need to be very accurate, but it can be slow and not scalable.
* A **Post-ranking model** : it selects offers from different categories and/or subcategories. The goal is to select a more equitable and diverse distribution of offers by categories (different artists, venues, types of offers, etc.). This combination can be done by categories, subcategories, GTL (book categorization), or by groups of clusters that have been previously trained on the semantics of the offers. The default case is clusters (500 clusters out of 4M items).


## The Retrieval Model

### Main Objective

- Extract, from a very large number of items (potentially millions), a limited subset of potentially relevant items for the user.
- This involves retrieving candidate offers for recommendations from the pass offer catalog (~10e6). Generally, this step allows going from ***N(~1e6)*** to ***k(~1e3)*** offers. Given that the offer catalog is very large, the Retrieval is often a *less precise but fast model*. We therefore choose a ***relatively high number of candidates*** to avoid "missing" any good recommendations (i.e., with an optimization of the recall metric).

### Common Techniques

- **Collaborative Filtering:** Uses the history of interactions (clicks, purchases, etc.) of similar users to identify items likely to interest the target user.
- **Content-based Filtering:** Compares item attributes with user preferences or profile.

### Expected Result

- A set of candidates (often a few hundred or thousand) that will be refined in the next step.

### What we do in practice
- We use up to 3 retrieval models to generate the candidates for the ranking model:
    - A model based on the user's interactions with the application and his profile. The algorithm used is the Two-Tower model which is the standard in the indutry. See the orginal [Youtube Recommendation Paper](https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/45530.pdf)
    - A top offers model, which is a model that retrieves the most popular offers.
    - A model based on the current trends.
- We apply this retrieval to 2 uses cases as presented above :
    - Home Recommendations : Given a user, we retrieve a list of offers that are likely to interest him.
    - Similar Offers Recommendations : Given an offer, we retrieve a list of offers that are similar to it.

### The Two-Tower Model
- **"Two-Tower" Models:** Two distinct neural networks are used: one to encode the user's profile and one to encode the items. The similarity (often via a dot product) between these two representations allows quickly selecting relevant items. The standard approach for this type of model is to:
        1. Calculate an Embedding model of Users **U** and Offers **O**, such that if user **i** has interacted with offer **j**, then the similarity of the two associated embeddings **U_i** and **O_j** is maximal, and minimal otherwise.
        2. Make a recommendation to the user:
            1. For a ***personalized recommendation to user $u$ (Home Recommendation)***: We perform a vector search of offers $O_j ~ , j \epsilon[1,k]$ such that $< U_u .O_j >$ is maximal. In practice, we use a search like [Approximate Nearest Neighbor](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (ANN) to speed up this phase.
            2. For a ***recommendation similar to offer $o$ (Similar Offer)***: We perform a vector search of offers $O_j ~ , j \epsilon[1,k]$ such that $< O_o .O_j >$ is maximal. In practice, we use a search like [Approximate Nearest Neighbor](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (ANN) to speed up this phase.


#### Overview

The Two-Tower approach represents a hybrid recommendation strategy, blending the strengths of traditional collaborative filtering and content-based recommendation systems. It achieves this by encoding both user and item features into dense embedding vectors, called **towers**.

In essence, it learns user preferences not just from their interactions with items, but also from the inherent properties of both users and items.

#### Feature Encoding (Layers)

* User and item features are converted into mathematical representations using **"layers"**
* Different layer types dictate how the features are "translated."
* embedding_size determines the vector dimension representing a feature.
* **Stacked layers form the user and item towers**



<!-- ##### Training and Validation

The model is trained using a combination of user interaction data, user profile information, and item metadata.

#### Training Frequency:
The model is retrained on a weekly basis to ensure it stays up-to-date with user behavior and catalog changes.

###### Data Window:

The training dataset includes:

Booking data from the past six months, capturing a broad view of user interests.


Enhanced focus on the most recent 3 months of click data to place greater weight on current user trends.

###### Training Process

ADD Laurent notes on training

Top-k metric, which evaluates the model's ability to rank consumed items highly among all items for a given user.
Optimization Goal: Maximize the rank of consumed items.
###### Key Distinction:
 Model uses click data for training, but booking (reservation) data for Validation. -->

| Training Aspects | Description |
| --- | ----------- |
|Training Process| Top-k metric, which evaluates the model's ability to rank consumed items highly among all items for a given user Optimization Goal: Maximize the rank of consumed items. |
| Validation | The model is trained using a combination of user interaction data, user profile information, and item metadata. |
| Frequency | The model is retrained on a weekly basis to ensure it stays up-to-date with user behavior and catalog changes. |
|Data Window| Booking data from the past six months and three months of click |
|**Key Distinction**| Model uses click data for training, but booking (reservation) data for Validation.|

### Offline Metrics
To evaluate our model we use a set of standard metrics, calculate by *microsoft recommenders*.

**Recall**
> The ability of the search to find all of the relevant items in the corpus in top k items.
consider both actual relevant and non-relevant results only from the returned items.

**Precision**
> The ability to retrieve top-ranked documents that are mostly relevant in top k items.

**Coverage**
> The ratio of recommended items with respect to
the total number of items in our training catalog

**Novelty**
>Novelty measures how new, original, or unusual the recommendations are for the user.

## The Ranking Model

In the context of recommendation, a ranking model is used to predict
the likelihood of specific user actions, such as clicking on or booking an item.
 By treating these actions as binary classification problems (e.g., click or no click, book or no book),
the model assigns probabilities to each item, indicating how likely a user is to interact with it.


### Classification model

The classification model leverages both user and item features,
such as user preferences, item metadata, and contextual information (e.g., time of day, distance to the offer).
These features are processed to train the model to distinguish between items
that are relevant to the user and those that are not.


### Click and booking prediction

We use this model to predicts both the probability of a click
and the probability of a booking.
The final score is simply the sum of these two probabilities.
 (it used to be more complex but we recently got back to a simpler model,
 which worked better, in order to later iterate on it).

 $$ s = P(click) + P(booking) $$

### The Data
To train this model, we leverage all interactions performed on the app's home page, including the offers a user has viewed, clicked on, or booked.

| Training Aspects | Description |
| --- | ----------- |
| Frequency | The model is retrained on a weekly basis to ensure it stays up-to-date with user behavior and catalog changes. |
|Data Window| Booking data from the past six months and three months of click |

### The metrics

## The Post-Ranking Model


### The metrics
