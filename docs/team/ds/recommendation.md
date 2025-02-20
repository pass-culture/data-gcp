
# The Pass Culture Recommendation Engine


We use the collaborative filtering technique to recommend cultural offers to users based on their interactions with the application.

**Collaborative filtering** is a technique used by recommender systems to make predictions about the interests of a user by collecting preferences from many users (collaborating), and extrapolating from that data to make predictions about the interests of one user.

Basically, we are training a model which allows to provide :
* Recommendations to a given user (we call it Home Recommendations)
* Similar offers to a given offer (we call it Similar Offers Recommendations)


## The Algorithms

### The pipeline architecture

We are using a Retrieval-Ranking model, which is a model that retrieves a list of items from a large corpus and ranks them in order of relevance to a given query, as you can see below.
![Retrieval-Ranking model](assets/retrieval-ranking-architecture.png)

Indeed, the model is composed of three parts:

* A **Retrieval model** : it retrieves a list of items from a very large corpus. Think from Millions to Hundreds.
    > The model need not to be very accurate, but it needs to be very fast and scalable.
* A **Ranking model** : it ranks the retrieved items in order of relevance to a given query. In our framework, we also use it to filter out the items that are not relevant to the user, so that we keep less than a hundred results.
    > The model need to be very accurate, but it can be slow and not scalable.
* A **Post-ranking model** : it selects offers from different categories and/or subcategories. The goal is to select a more equitable and diverse distribution of offers by categories (different artists, venues, types of offers, etc.). This combination can be done by categories, subcategories, GTL (book categorization), or by groups of clusters that have been previously trained on the semantics of the offers. The default case is clusters (500 clusters out of 4M items).


### The Retrieval Model

We use a two-tower model which is the standard in the indutry. See the orginal [Youtube Recommendation Paper](https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/45530.pdf)

#### The Two-Tower Model

TODO

#### The Data

TODO

#### Offline Metrics


### The Ranking Model

TODO

#### Click and booking prediction model

We use a model that predicts both the probability of a click and the probability of a booking. The final score is simply the sum of these two probabilities. (it used to be more complex but we recently got back to a simpler model, which worked better, in order to later iterate on it).

s = P(click) + P(booking)

#### The Data

TODO

#### Metrics

TODO

### The Post-Ranking Model


## The metrics

TODO
