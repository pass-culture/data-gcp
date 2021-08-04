import random
import numpy as np
from scipy.spatial.distance import cosine
from datetime import datetime

NUMBER_OF_USERS = 10000

TYPE_LIST = [
    "ThingType.LIVRE_EDITION",
    "ThingType.INSTRUMENT",
    "EventType.PRATIQUE_ARTISTIQUE",
    "ThingType.AUDIOVISUEL",
    "ThingType.MUSIQUE",
    "EventType.SPECTACLE_VIVANT",
    "ThingType.PRATIQUE_ARTISTIQUE_ABO",
    "EventType.CINEMA",
    "EventType.MUSIQUE",
    "EventType.CONFERENCE_DEBAT_DEDICACE",
    "EventType.MUSEES_PATRIMOINE",
    "ThingType.MUSEES_PATRIMOINE_ABO",
    "ThingType.PRESSE_ABO",
    "ThingType.OEUVRE_ART",
    "ThingType.LIVRE_AUDIO",
    "EventType.JEUX",
    "ThingType.CINEMA_ABO",
    "ThingType.JEUX_VIDEO",
    "ThingType.CINEMA_CARD",
    "ThingType.MUSIQUE_ABO",
    "EventType.ACTIVATION",
    "ThingType.JEUX_VIDEO_ABO",
    "ThingType.SPECTACLE_VIVANT_ABO",
    "ThingType.ACTIVATION",
]


def get_unexpectedness(booked_type_list, recommended_type_list):
    booked_type_vector_list = [
        [int(booked_type == offer_type) for offer_type in TYPE_LIST]
        for booked_type in booked_type_list
    ]
    recommended_type_vector_list = [
        [int(recommended_type == offer_type) for offer_type in TYPE_LIST]
        for recommended_type in recommended_type_list
    ]

    cosine_sum = 0
    for booked_type_vector in booked_type_vector_list:
        for recommended_type_vector in recommended_type_vector_list:
            cosine_sum += cosine(booked_type_vector, recommended_type_vector)

    return (1 / (len(booked_type_list) * len(recommended_type_list))) * cosine_sum


def compute_metrics(k, positive_data_train, positive_data_test, match_model):
    print("time0 (start) :", datetime.now().isoformat())
    # Map all offers to corresponding types
    offer_type_dict = {}
    unique_offer_types = (
        positive_data_train.groupby(["item_id", "type"]).first().reset_index()
    )
    for item_id, item_type in zip(
        unique_offer_types.item_id.values, unique_offer_types.type.values
    ):
        offer_type_dict[item_id] = item_type

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
    new_types_ratio = []

    random_users_to_test = random.sample(all_test_user_ids, NUMBER_OF_USERS)
    for user_id in random_users_to_test:
        print("#############")
        user_count += 1
        print("time 1:", datetime.now().isoformat())
        positive_item_train = positive_data_train[
            positive_data_train["user_id"] == user_id
        ]
        positive_item_test = cleaned_positive_data_test[
            cleaned_positive_data_test["user_id"] == user_id
        ]
        print("time 2:", datetime.now().isoformat())
        # Remove items in train - they can not be in test set anyway
        items_to_rank = np.setdiff1d(
            all_item_ids, positive_item_train["item_id"].values
        )
        booked_offer_types = list(positive_item_train["type"].values)

        # Check if any item of items_to_rank is in the test positive feedback for this user
        expected = np.in1d(items_to_rank, positive_item_test["item_id"].values)

        repeated_user_id = np.empty_like(items_to_rank)
        repeated_user_id.fill(user_id)
        print("time 3:", datetime.now().isoformat())
        predicted = match_model.predict(
            [repeated_user_id, items_to_rank], batch_size=4096
        )
        print("time 4:", datetime.now().isoformat())
        scored_items = sorted(
            [(item_id, score[0]) for item_id, score in zip(items_to_rank, predicted)],
            key=lambda k: k[1],
            reverse=True,
        )[:k]
        print("time 5:", datetime.now().isoformat())
        recommended_offer_types = [offer_type_dict[item[0]] for item in scored_items]

        if booked_offer_types and recommended_offer_types:
            user_unexpectedness = get_unexpectedness(
                booked_offer_types, recommended_offer_types
            )
            unexpectedness.append(user_unexpectedness)
            new_types_ratio.append(
                np.mean(
                    [
                        int(recommended_offer_type not in booked_offer_types)
                        for recommended_offer_type in recommended_offer_types
                    ]
                )
            )
        print("time 6:", datetime.now().isoformat())
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

            if hidden_items and booked_offer_types and recommended_offer_types:
                user_serendipity = (
                    (len(recommended_hidden_items) / len(hidden_items))
                    * user_unexpectedness
                    * 100
                )
                serendipity.append(user_serendipity)
        print("time 7:", datetime.now().isoformat())
        metrics = {
            f"recall_at_{k}": (recommended_hidden_items_number / hidden_items_number)
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
            f"new_types_ratio_at_{k}": np.mean(new_types_ratio)
            if len(new_types_ratio) > 0
            else None,
            f"serendipity_at_{k}": np.nanmean(serendipity)
            if len(serendipity) > 0
            else None,
        }

        if user_count % 100 == 0:
            print(f"Metrics at user number {user_count} / {len(random_users_to_test)}")
            print(metrics)

    return metrics
