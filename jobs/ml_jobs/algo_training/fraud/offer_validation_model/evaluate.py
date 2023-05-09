import pandas as pd
import mlflow
import json
from utils.constants import (
    MODEL_DIR,
    STORAGE_PATH,
    CONFIGS_PATH,
    MLFLOW_CLIENT_ID,
)
from fraud.offer_validation_model.utils.tools import (
    read_from_gcs,
    connect_remote_mlflow,
    get_confusion_matrix,
    plot_matrix,
)


def evaluate(
    experiment_name: str = typer.Option(
        EXPERIMENT_NAME, help="Name of the experiment on MLflow"
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    input_evaluation_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert numerical columns to int
    """
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    eval_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_evaluation_table_name
    )

    eval_data_labels = eval_data.target.tolist()
    eval_data = eval_data.drop(columns=["target"])
    eval_pool = Pool(
        eval_data,
        eval_data_labels,
        cat_features=features["catboost_features_types"]["cat_features"],
        text_features=features["catboost_features_types"]["text_features"],
        embedding_features=features["catboost_features_types"]["embedding_features"],
    )

    connect_remote_mlflow(MLFLOW_CLIENT_ID)
    model = mlflow.catboost.load_model(
        model_uri=f"models:/validation_model_test/Staging"
    )
    metrics = model.eval_metrics(
        eval_pool,
        ["Accuracy", "BalancedAccuracy", "Precision", "Recall", "BalancedErrorRate"],
        ntree_start=0,
        ntree_end=1,
        eval_period=1,
        thread_count=-1,
        log_cout=sys.stdout,
        log_cerr=sys.stderr,
    )
    # Format metrics for MLFlow
    for key in metrics.keys():
        metrics[key] = metrics[key][0]

    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment, run_name=run_name):
        mlflow.log_metrics(metrics)


# TO ADD for extra evaluation plots
# ## Confusion matrix

# cm=get_confusion_matrix(model_loaded, eval_pool)
# cmn=[[0,0],[0,0]]
# cmn[0][0]=cm[0][0]/(cm[0][0]+cm[0][1])
# cmn[0][1]=cm[0][1]/(cm[0][0]+cm[0][1])
# cmn[1][0]=cm[1][0]/(cm[1][0]+cm[1][1])
# cmn[1][1]=cm[1][1]/(cm[1][0]+cm[1][1])


# classes = ['Rejected', 'Validated']
# title = "Offer Validation"
# plot_matrix(cmn, classes, title)

# ## Feature importance

# feature_importance=model_loaded.get_feature_importance(data=train_pool,
#                     reference_data=None,
#                     prettified=False,
#                     thread_count=-1,
#                     verbose=False)
# sorted_idx = np.argsort(feature_importance)
# fig = plt.figure(figsize=(10, 4))
# plt.barh(range(len(sorted_idx)), feature_importance[sorted_idx], align='center')
# plt.yticks(range(len(sorted_idx)), np.array(train_data.columns)[sorted_idx])
# plt.title('Feature Importance')

# ## Shap Values
# shap_values = model_loaded.get_feature_importance(test_pool, type="ShapValues")
# shap_values = shap_values[:,:-1]
# shap.summary_plot(shap_values, test_data,plot_size=(10,4))

# ## Probability density

# #Get prediction as PROBABILITY

# predict_test_prod_proba=model_loaded.predict(eval_pool,
#         prediction_type='Probability',
#         ntree_start=0,
#         ntree_end=0,
#         thread_count=-1,
#         verbose=None)
# proba_rej=[prob[0] for prob in list(predict_test_prod_proba)]
# proba_val=[prob[1] for prob in list(predict_test_prod_proba)]
# eval_data["target"]=eval_data_labels
# eval_data["probability_rejected"]=proba_rej
# eval_data["probability_validated"]=proba_val

# eval_data.query("target==1").probability_validated.hist()
# eval_data.query("target==0").probability_validated.hist()
