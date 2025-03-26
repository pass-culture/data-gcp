# The Pass Culture Recommendation Engine

Our recommendation engine aims to highlight, from our rich base of **2 million cultural goods and events**, relevant and personalized items to our **xM users**.

We use the **collaborative filtering** technique to recommend cultural offers to users based on their interactions with the application.

> **Collaborative filtering** is a technique used by recommender systems to make predictions about the interests of a user by collecting preferences from many users (collaborating) and extrapolating from that data to make predictions about the interests of one user.

### Key Features:
- **Home Recommendations**: Recommendations tailored to a given user.
- **Similar Offers Recommendations**: Suggestions for similar offers to a given offer.

---

## The Algorithms

### The Pipeline Architecture

We use a **Retrieval-Ranking model**, which retrieves a list of items from a large corpus and ranks them in order of relevance to a given query.

![Retrieval-Ranking model](assets/retrieval-ranking-architecture.png)

The model is composed of three parts:

1. **Retrieval Model**: Retrieves a list of items from a very large corpus (millions to hundreds).
   - Needs to be **fast** and **scalable**, but not necessarily very accurate.
2. **Ranking Model**: Ranks the retrieved items in order of relevance to a given query.
   - Needs to be **accurate**, but can be slower and less scalable.
3. **Post-Ranking Model**: Selects offers from different categories and/or subcategories to ensure a more equitable and diverse distribution of offers.

---

### The Retrieval Model

We use a **Two-Tower Model**, which is the standard in the industry. See the original [YouTube Recommendation Paper](https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/45530.pdf).

#### The Two-Tower Model

**Overview**:
The Two-Tower approach blends the strengths of collaborative filtering and content-based recommendation systems by encoding both user and item features into dense embedding vectors (**towers**).

**Feature Encoding (Layers)**:
- User and item features are converted into mathematical representations using **layers**.
- The `embedding_size` determines the vector dimension representing a feature.
- **Stacked layers** form the user and item towers.

---

### Training and Validation

The model is trained using a combination of:
- **User interaction data**
- **User profile information**
- **Item metadata**

#### Training Frequency:
- The model is retrained **weekly** to stay up-to-date with user behavior and catalog changes.

#### Data Window:
- **Booking data** from the past **6 months** to capture a broad view of user interests.
- Enhanced focus on the most recent **3 months** of **click data** to place greater weight on current user trends.

#### Training Process:
- **Top-k metric**: Evaluates the model's ability to rank consumed items highly among all items for a given user.
- **Optimization Goal**: Maximize the rank of consumed items.

> **Key Distinction**:
> The model uses **click data** for training but **booking (reservation) data** for validation.

---

### The Ranking Model

In the context of recommendation, a **classification model** is used to predict the likelihood of specific user actions, such as clicking on or booking an item.

#### Classification Model

- Leverages **user and item features**, such as:
  - User preferences
  - Item metadata
  - Contextual information (e.g., time of day, distance to the offer)
- Processes these features to distinguish between items that are relevant to the user and those that are not.
- The output is used as a **ranking signal**, where items with higher predicted probabilities are ranked higher.

---

#### Click and Booking Prediction Model

We use this model to predict:
- The probability of a **click**.
- The probability of a **booking**.

The final score is calculated as:

$$ s = P(click) + P(booking) $$

> This simpler model has shown better performance and allows for easier iteration.

---

### The Post-Ranking Model

The **Post-Ranking Model** ensures diversity and fairness by selecting offers from different categories, subcategories, or clusters. This helps provide a more equitable distribution of offers, such as:
- Different artists
- Venues
- Types of offers

---

### Metrics

#### Offline Metrics:
- **Top-k Accuracy**: Measures how well the model ranks relevant items in the top-k results.
- **Diversity Metrics**: Ensures a varied selection of offers in the recommendations.
- **Engagement Metrics**: Tracks user interactions with the recommended items.

---

### Summary

The Pass Culture Recommendation Engine combines **collaborative filtering**, **retrieval-ranking models**, and **post-ranking diversification** to deliver personalized and diverse recommendations to users. By continuously retraining and validating the model, we ensure that it adapts to changing user behavior and catalog updates.
