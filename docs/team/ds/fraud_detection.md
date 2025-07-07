# Fraud Detection
# Offer Compliance Classification Model

## Overview

This document outlines the development and evaluation of a classification model designed to automate and streamline the manual compliance review process for offers on the "Pass Culture" platform. The model aims to predict whether an offer will be validated or rejected based on various offer attributes, thereby reducing the manual workload.

## Context

Manually created offers on the "Pass Culture" platform are subjected to a compliance scoring script. Offers that trigger alerts require manual review, leading to a significant volume of offers needing analysis.

## Data

### Dataset

* The dataset comprises offers that have undergone manual review
* The dataset contains approximately 230,000 offers, with a class imbalance of ~90% validated and ~10% rejected.

### Features

The following offer attributes were used as features:

* **Textual:**
    * `offer_name`
    * `offer_description`
* **Categorical:**
    * `offer_subcategory_id`
    * `venue_department_code`
* **Numerical:**
    * `stock`
    * `stock_price`
* **Boolean:**
    * `outing`
    * `physical_goods`
* **Other:**
    * `type`
    * `subType`
    * `rayon` (product category)
    * `macro_rayon` (broader product category)
    * Offer image

### Feature Engineering

* **Textual Features:** Processed directly by CatBoost.
* **Categorical Features:** One-hot encoded.
* **Numerical Features:** Used as is.
* **Image Features:** Embedded using a pre-trained model.

## Model

### Model Architecture

* The model uses the `CatBoostClassifier` from the CatBoost library, which is well-suited for handling a mix of numerical, categorical, and textual features.

### Training

* The model was trained on the manually reviewed offer dataset.
* Training temporality is to be reviewed for improvement.

### Evaluation

* The model was evaluated using a test set of 22,882 offers (20,187 validated, 2,695 rejected).

### Metrics

* **Accuracy:** 0.95
* **Balanced Accuracy:** 0.81
* **Precision:** 0.95
* **Recall:** 0.99
* **Balanced Error Rate:** 0.18

### Confusion Matrix

* Generated from the test set.

### Feature Importance

* **SHAP Values:** Used to estimate the contribution of each feature to the model's predictions.
* **Feature Analysis:** Detailed analysis available in the associated notebook.
* **Probability Density:** Visualization of the probability of offers being validated or rejected, enabling threshold optimization.
