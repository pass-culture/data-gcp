import random
import numpy as np
import tensorflow as tf
import warnings
from scipy.spatial.distance import cosine
from operator import itemgetter
from utils import ENV_SHORT_NAME

NUMBER_OF_USERS = 5000 if ENV_SHORT_NAME == "prod" else 200

TYPE_LIST = [
    "ABO_BIBLIOTHEQUE",
    "ABO_CONCERT",
    "ABO_JEU_VIDEO",
    "ABO_LIVRE_NUMERIQUE",
    "ABO_LUDOTHEQUE",
    "ABO_MEDIATHEQUE",
    "ABO_MUSEE",
    "ABO_PLATEFORME_MUSIQUE",
    "ABO_PLATEFORME_VIDEO",
    "ABO_PRATIQUE_ART",
    "ABO_PRESSE_EN_LIGNE",
    "ABO_SPECTACLE",
    "ACHAT_INSTRUMENT",
    "ACTIVATION_EVENT",
    "ACTIVATION_THING",
    "APP_CULTURELLE",
    "ATELIER_PRATIQUE_ART",
    "AUTRE_SUPPORT_NUMERIQUE",
    "BON_ACHAT_INSTRUMENT",
    "CAPTATION_MUSIQUE",
    "CARTE_CINE_ILLIMITE",
    "CARTE_CINE_MULTISEANCES",
    "CARTE_MUSEE",
    "CINE_PLEIN_AIR",
    "CINE_VENTE_DISTANCE",
    "CONCERT",
    "CONCOURS",
    "CONFERENCE",
    "DECOUVERTE_METIERS",
    "ESCAPE_GAME",
    "EVENEMENT_CINE",
    "EVENEMENT_JEU",
    "EVENEMENT_MUSIQUE",
    "EVENEMENT_PATRIMOINE",
    "FESTIVAL_CINE",
    "FESTIVAL_LIVRE",
    "FESTIVAL_MUSIQUE",
    "FESTIVAL_SPECTACLE",
    "JEU_EN_LIGNE",
    "JEU_SUPPORT_PHYSIQUE",
    "LIVESTREAM_EVENEMENT",
    "LIVESTREAM_MUSIQUE",
    "LIVRE_AUDIO_PHYSIQUE",
    "LIVRE_NUMERIQUE",
    "LIVRE_PAPIER",
    "LOCATION_INSTRUMENT",
    "MATERIEL_ART_CREATIF",
    "MUSEE_VENTE_DISTANCE",
    "OEUVRE_ART",
    "PARTITION",
    "PODCAST",
    "RENCONTRE_JEU",
    "RENCONTRE",
    "SALON",
    "SEANCE_CINE",
    "SEANCE_ESSAI_PRATIQUE_ART",
    "SPECTACLE_ENREGISTRE",
    "SPECTACLE_REPRESENTATION",
    "SUPPORT_PHYSIQUE_FILM",
    "SUPPORT_PHYSIQUE_MUSIQUE",
    "TELECHARGEMENT_LIVRE_AUDIO",
    "TELECHARGEMENT_MUSIQUE",
    "VISITE_GUIDEE",
    "VISITE_VIRTUELLE",
    "VISITE",
    "VOD",
]


def get_unexpectedness(booked_subcategoryId_list, recommended_subcategoryId_list):
    booked_subcategoryId_vector_list = [
        [
            int(booked_subcategoryId == offer_subcategoryId)
            for offer_subcategoryId in TYPE_LIST
        ]
        for booked_subcategoryId in booked_subcategoryId_list
    ]
    recommended_subcategoryId_vector_list = [
        [
            int(recommended_subcategoryId == offer_subcategoryId)
            for offer_subcategoryId in TYPE_LIST
        ]
        for recommended_subcategoryId in recommended_subcategoryId_list
    ]

    cosine_sum = 0
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for booked_subcategoryId_vector in booked_subcategoryId_vector_list:
            for (
                recommended_subcategoryId_vector
            ) in recommended_subcategoryId_vector_list:
                cosine_sum += cosine(
                    booked_subcategoryId_vector, recommended_subcategoryId_vector
                )

    return (
        1 / (len(booked_subcategoryId_list) * len(recommended_subcategoryId_list))
    ) * cosine_sum


def compute_metrics(k, positive_data_train, positive_data_test, model_name, model):
    # Map all offers to corresponding subcategoryIds
    offer_subcategoryId_dict = {}
    if model_name == "v2_mf_reco":
        positive_data_train.rename(columns={"offer_id": "item_id"}, inplace=True)
        positive_data_test.rename(columns={"offer_id": "item_id"}, inplace=True)

    unique_offer_subcategoryIds = (
        positive_data_train.groupby(["item_id", "offer_subcategoryid"])
        .first()
        .reset_index()
    )
    for item_id, item_subcategoryId in zip(
        unique_offer_subcategoryIds.item_id.values,
        unique_offer_subcategoryIds.offer_subcategoryid.values,
    ):
        offer_subcategoryId_dict[item_id] = item_subcategoryId

    # Only keep user - item interactions in positive_data_test, which can be inferred from model
    cleaned_positive_data_test = positive_data_test.copy()
    print(
        f"Original number of positive feedbacks in test: {cleaned_positive_data_test.shape[0]}"
    )

    cleaned_positive_data_test = cleaned_positive_data_test[
        cleaned_positive_data_test.user_id.isin(positive_data_train.user_id)
    ]
    print(
        f"Number of positive feedbacks in test after removing users not present in train: {cleaned_positive_data_test.shape[0]}"
    )

    cleaned_positive_data_test = cleaned_positive_data_test[
        cleaned_positive_data_test.item_id.isin(positive_data_train.item_id)
    ]
    print(
        f"Number of positive feedbacks in test after removing offers not present in train: {cleaned_positive_data_test.shape[0]}"
    )

    # Get all offers the model may predict
    all_item_ids = list(set(positive_data_train.item_id.values))
    # Get all users in cleaned test data
    all_test_user_ids = list(set(cleaned_positive_data_test.user_id.values))

    hidden_items_number = 0
    recommended_hidden_items_number = 0
    prediction_number = 0
    recommended_items = []

    user_count = 0
    unexpectedness = []
    serendipity = []
    new_subcategoryIds_ratio = []

    if len(all_test_user_ids) > NUMBER_OF_USERS:
        random_users_to_test = random.sample(all_test_user_ids, NUMBER_OF_USERS)
    else:
        random_users_to_test = all_test_user_ids
    for user_id in random_users_to_test:
        user_count += 1
        positive_item_train = positive_data_train[
            positive_data_train["user_id"] == user_id
        ]
        positive_item_test = cleaned_positive_data_test[
            cleaned_positive_data_test["user_id"] == user_id
        ]
        # Remove items in train - they can not be in test set anyway
        items_to_rank = np.setdiff1d(
            all_item_ids, positive_item_train["item_id"].values
        )
        booked_offer_subcategoryIds = list(
            positive_item_train["offer_subcategoryid"].values
        )

        # Check if any item of items_to_rank is in the test positive feedback for this user
        expected = np.in1d(items_to_rank, positive_item_test["item_id"].values)

        repeated_user_id = np.array([user_id] * len(items_to_rank))
        print("len(repeated_user_id)", len(repeated_user_id))
        print("len(items_to_rank)", len(items_to_rank))
        if model_name == "v1":
            predicted = model.predict(
                [repeated_user_id, items_to_rank], batch_size=4096
            )
        if model_name == "v2_deep_reco":
            items_to_rank_subcategoryIds = np.array(
                [offer_subcategoryId_dict[item_id] for item_id in items_to_rank]
            )
            predicted = model.predict(
                [repeated_user_id, items_to_rank, items_to_rank_subcategoryIds],
                batch_size=4096,
            )
        if model_name == "v2_mf_reco":
            predicted = model.predict(
                [repeated_user_id, items_to_rank],
                batch_size=4096,
            )

        scored_items = sorted(
            [(item_id, score[0]) for item_id, score in zip(items_to_rank, predicted)],
            key=itemgetter(1),
            reverse=True,
        )[:k]
        recommended_offer_subcategoryIds = [
            offer_subcategoryId_dict[item[0]] for item in scored_items
        ]

        if booked_offer_subcategoryIds and recommended_offer_subcategoryIds:
            user_unexpectedness = get_unexpectedness(
                booked_offer_subcategoryIds, recommended_offer_subcategoryIds
            )
            unexpectedness.append(user_unexpectedness)
            new_subcategoryIds_ratio.append(
                np.mean(
                    [
                        int(
                            recommended_offer_subcategoryId
                            not in booked_offer_subcategoryIds
                        )
                        for recommended_offer_subcategoryId in recommended_offer_subcategoryIds
                    ]
                )
            )
        if np.sum(expected) >= 1:
            recommended_items.extend([item[0] for item in scored_items])
            recommended_items = list(set(recommended_items))

            hidden_items = list(positive_item_test["item_id"].values)
            recommended_hidden_items = [
                item[0] for item in scored_items if item[0] in hidden_items
            ]

            hidden_items_number += len(hidden_items)
            recommended_hidden_items_number += len(recommended_hidden_items)
            prediction_number += 1

            if (
                hidden_items
                and booked_offer_subcategoryIds
                and recommended_offer_subcategoryIds
            ):
                user_serendipity = (
                    (len(recommended_hidden_items) / len(hidden_items))
                    * user_unexpectedness
                    * 100
                )
                serendipity.append(user_serendipity)

        if user_count % 100 == 0:
            metrics = {
                f"recall_at_{k}": (
                    recommended_hidden_items_number / hidden_items_number
                )
                * 100
                if hidden_items_number > 0
                else None,
                f"precision_at_{k}": (
                    recommended_hidden_items_number / (prediction_number * k)
                )
                * 100,
                f"maximal_precision_at_{k}": (
                    cleaned_positive_data_test.shape[0] / (prediction_number * k)
                )
                * 100,
                f"coverage_at_{k}": (len(recommended_items) / len(all_item_ids)) * 100,
                f"unexpectedness_at_{k}": np.nanmean(unexpectedness)
                if len(unexpectedness) > 0
                else None,
                f"new_types_ratio_at_{k}": np.mean(new_subcategoryIds_ratio)
                if len(new_subcategoryIds_ratio) > 0
                else None,
                f"serendipity_at_{k}": np.nanmean(serendipity)
                if len(serendipity) > 0
                else None,
            }

            print(f"Metrics at user number {user_count} / {len(random_users_to_test)}")
            print(metrics)

    metrics = {
        f"recall_at_{k}": (recommended_hidden_items_number / hidden_items_number) * 100
        if hidden_items_number > 0
        else None,
        f"precision_at_{k}": (recommended_hidden_items_number / (prediction_number * k))
        * 100,
        f"maximal_precision_at_{k}": (
            cleaned_positive_data_test.shape[0] / (prediction_number * k)
        )
        * 100,
        f"coverage_at_{k}": (len(recommended_items) / len(all_item_ids)) * 100,
        f"unexpectedness_at_{k}": np.nanmean(unexpectedness)
        if len(unexpectedness) > 0
        else None,
        f"new_types_ratio_at_{k}": np.mean(new_subcategoryIds_ratio)
        if len(new_subcategoryIds_ratio) > 0
        else None,
        f"serendipity_at_{k}": np.nanmean(serendipity)
        if len(serendipity) > 0
        else None,
    }

    return metrics
