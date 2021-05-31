import json
import random

import numpy as np
import pandas as pd
import tensorflow as tf

from metrics import compute_metrics
from tf_model import TripletModel, MatchModel, identity_loss
from matplotlib import pyplot as plt

MODEL_DATA_PATH = "tf_bpr_string_input_5_months_reg_0"
START_DATE = "2020-12-10"
END_DATE = "2021-05-10"
EMBEDDING_SIZE = 64
L2_REG = 0  # 1e-6

n_epochs = 20
batch_size = 32


def save_dict_to_path(dictionnary, path):
    with open(path, "w") as fp:
        json.dump(dictionnary, fp)


def get_bookings(start_date, end_date):
    query = f"""
        select user_id,
        (CASE WHEN offer.offer_type in ('ThingType.LIVRE_EDITION', 'EventType.CINEMA') THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) END) AS offer_id, offer.offer_type as type,
        count(*) as nb_bookings
        from `passculture-data-prod.clean_prod.applicative_database_booking` booking
        inner join `passculture-data-prod.clean_prod.applicative_database_stock` stock
        on booking.stock_id = stock.stock_id
        inner join `passculture-data-prod.clean_prod.applicative_database_offer` offer
        on stock.offer_id = offer.offer_id
        where offer.offer_creation_date >= DATETIME '{start_date} 00:00:00'
        and offer.offer_creation_date <= DATETIME '{end_date} 00:00:00'
        group by user_id, offer_id, type
    """
    bookings = pd.read_gbq(query)
    return bookings


# DATA PROCESSING
bookings = get_bookings(start_date=START_DATE, end_date=END_DATE).rename(
    columns={"offer_id": "item_id", "nb_bookings": "rating"}
)
print("Bookings:")
print(bookings.head())
n_users = len(set(bookings.user_id.values))
n_items = len(set(bookings.item_id.values))
print(f"{n_users} users and {n_items} items")
user_ids = bookings["user_id"].unique().tolist()
item_ids = bookings["item_id"].unique().tolist()

# SPLIT TRAIN AND TEST DATA
df = bookings.sample(frac=1).reset_index(drop=True)
lim_train = df.shape[0] * 80 / 100
lim_eval = df.shape[0] * 90 / 100
pos_data_train = df.loc[df.index < lim_train]
pos_data_eval = df.loc[df.index < lim_eval]
pos_data_eval = pos_data_eval.loc[pos_data_eval.index >= lim_train]
pos_data_test = df[df.index >= lim_eval]
pos_data_train.to_csv(f"{MODEL_DATA_PATH}/pos_data_train.csv", index=False)
pos_data_test.to_csv(f"{MODEL_DATA_PATH}/pos_data_test.csv", index=False)

triplet_model = TripletModel(
    user_ids, item_ids, latent_dim=EMBEDDING_SIZE, l2_reg=L2_REG
)
match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)

user_id = "19373"
items_to_rank = np.array(
    ["offer-7514002", "product-2987109", "offer-6406524", "toto", "tata"]
)
repeated_user_id = np.empty_like(items_to_rank)
repeated_user_id.fill(user_id)
predicted = match_model.predict([repeated_user_id, items_to_rank], batch_size=4096)
print(predicted)


def sample_triplets(pos_data, item_ids):
    """Sample negatives at random"""

    user_ids = pos_data["user_id"].values
    pos_item_ids = pos_data["item_id"].values
    neg_item_ids = np.array(random.choices(item_ids, k=len(user_ids)), dtype=object)

    return [user_ids, pos_item_ids, neg_item_ids]


fake_y = np.array(["1"] * pos_data_train["user_id"].shape[0], dtype=object)
evaluation_fake_train = np.array(
    ["1"] * pos_data_eval["user_id"].shape[0], dtype=object
)
triplet_model.compile(loss=identity_loss, optimizer="adam")

best_eval = 1

train = []
evaluation = []
runned_epochs = 0
for i in range(n_epochs):
    # Sample new negatives to build different triplets at each epoch
    triplet_inputs = sample_triplets(pos_data_train, item_ids)
    evaluation_triplet_inputs = sample_triplets(pos_data_eval, item_ids)
    # ds = tf.data.Dataset.from_tensor_slices(triplet_inputs)

    # Fit the model incrementally by doing a single pass over the
    # sampled triplets.
    try:
        print(f"Training epoch {i}")
        train_result = triplet_model.fit(
            x=triplet_inputs, y=fake_y, shuffle=True, batch_size=64, epochs=1
        )
        train.append(train_result.history["loss"][0])
        print(train)
        print(f"Evaluation epoch {i}")
        eval_result = triplet_model.evaluate(
            x=evaluation_triplet_inputs, y=evaluation_fake_train, batch_size=64
        )
        evaluation.append(eval_result)
        print(evaluation)
        runned_epochs += 1
        if eval_result < best_eval:
            tf.saved_model.save(match_model, f"{MODEL_DATA_PATH}/tf_bpr_{i}epochs")
            best_eval = eval_result
            best_model_path = f"{MODEL_DATA_PATH}/tf_bpr_{i}epochs"
    except KeyboardInterrupt:
        break

# TRAINING CURVES
plt.plot(list(range(runned_epochs)), train[:runned_epochs], label="Train Loss")
plt.plot(
    list(range(runned_epochs)), evaluation[:runned_epochs], label="Evaluation Loss"
)
plt.xlabel("Epoch")
plt.ylabel("Losses")
plt.legend()
plt.savefig(f"{MODEL_DATA_PATH}/learning_curves.png")

best_model_loaded = tf.saved_model.load(best_model_path)
metrics = compute_metrics(10, pos_data_train, pos_data_test, best_model_loaded)

print(metrics)
save_dict_to_path(metrics, f"{MODEL_DATA_PATH}/metrics.json")
