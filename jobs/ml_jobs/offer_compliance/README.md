# Offer Compliance ML Model

This project implements a machine learning workflow for automatic offer compliance validation. It uses a CatBoost classifier model to predict whether an offer should be approved or rejected based on various features.

## Overview

The offer compliance model analyzes various offer attributes (name, description, images, etc.) to determine if an offer complies with platform policies. It helps automate the compliance validation process by identifying offers that are likely to be rejected.

## Project Structure

- `train.py`: Trains the CatBoost model using the preprocessed training data
- `preprocess.py`: Prepares raw data for model training by cleaning and transforming features
- `evaluate.py`: Evaluates model performance on validation data and logs metrics to MLflow
- `package_api_model.py`: Packages the trained model for API deployment
- `split_data.py`: Splits data into training and validation sets

## Data Flow

1. Raw offer data is extracted from BigQuery as parquet files
2. Data preprocessing transforms the raw data into a format suitable for model training
3. Data is split into training and validation sets
4. The model is trained using the CatBoost algorithm
5. Model performance is evaluated using validation metrics
6. The model is packaged for API deployment and registered in MLflow

## Features

The model uses several types of features:
- Text features (offer name, description, etc.)
- Categorical features (category ID, rayon, etc.)
- Numerical features (price, etc.)
- Embeddings features (image embeddings, text embeddings)

## Configuration

Model configuration is specified in JSON files located in the `CONFIGS_PATH` directory.
These config files define:
- Feature types for preprocessing
- CatBoost feature configurations (categorical, text, embedding)
- Model hyperparameters

Available configuration files:
- `default.json`: Standard configuration using raw text features with CatBoost text processing
- `pretrained_feature_encoding.json`: Uses pre-trained embeddings for semantic content

## Data Splitting

The `split_data.py` script handles the division of the cleaned dataset into separate partitions:

- Training data (80% of the original dataset)
- Evaluation/validation data (10% of the original dataset)
- Test data (10% of the original dataset, not saved separately)

The data is randomly shuffled before splitting to ensure an unbiased division. The resulting training and validation datasets are saved as parquet files in the specified storage location.

To run the data splitting process:
```
python split_data.py --clean-table-name <clean_data_table> --training-table-name <output_training_table> --validation-table-name <output_validation_table>
```

## Workflow Execution

The workflow is orchestrated using Airflow. The DAG `algo_training_offer_compliance_model`
handles the complete pipeline execution.

### Manual Execution

To run individual components:

1. Preprocess data:
    ```
    python preprocess.py --config-file-name <config_file> --input-dataframe-file-name <input> --output-dataframe-file-name <output>
    ```
2. Split data:
    ```
    python split_data.py --clean-table-name <clean_data_table> --training-table-name <output_training_table> --validation-table-name <output_validation_table>
    ```
3. Train model:
    ```
    python train.py --model-name <model_name> --config-file-name <config_file> --training-table-name <training_data>
    ```
4. Evaluate model:
    ```
    python evaluate.py --model-name <model_name> --config-file-name <config_file> --validation-table-name <validation_data>
    ```
5. Package model:
    ```
    python package_api_model.py --model-name <model_name> --config-file-name <config_file>
    ```

## MLflow Integration

The project uses MLflow for experiment tracking and model registry:
- Training metrics are logged to MLflow
- The trained model is registered in the MLflow model registry
- Model versions are managed with staging/production tags
- API models are packaged and registered with proper metadata

## API Model

The packaged API model includes:
- A preprocessing pipeline with text and image encoders
- A classification pipeline for prediction
- Explainability features that provide the main contributors to a prediction
