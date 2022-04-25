import pandas as pd
import time
import queue
import threading

exitFlag = 0

from tools.utils import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    DATA_GCS_BUCKET_NAME,
)
from tools.diversification_kpi import (
    calculate_diversification_per_feature,
    fuse_columns_into_format,
)


def count_data():
    query = f"""SELECT count(DISTINCT user_id) as nb
        FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data 
        WHERE user_total_deposit_amount = 300
        LIMIT 10000"""
    count = pd.read_gbq(query)
    return count.iloc[0]["nb"]


def get_batch_of_users(batch, batch_size):
    query = f"""SELECT DISTINCT user_id
            FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data 
            WHERE user_total_deposit_amount = 300
            ORDER BY user_id
            LIMIT {batch_size} OFFSET {batch * batch_size}"""
    users_batch = pd.read_gbq(query)
    return users_batch


def get_data(users_batch):
    where_user_in = f"""A.user_id IN ("""
    for user in users_batch["user_id"]:
        where_user_in = where_user_in + f"'{user}',"
    where_user_in = where_user_in[:-1] + ")"

    query = f"""SELECT DISTINCT A.user_id, bkg.booking_creation_date, bkg.booking_id, user_region_name, user_activity,
    user_civility, user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent, offer.offer_id, booking_amount,
    offer_category_id as category, bkg.offer_subcategoryId as subcategory, bkg.physical_goods,
    bkg.digital_goods, bkg.event, offer.genres, offer.rayon, offer.type, offer.venue_id, offer.venue_name,C.*
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data A
    INNER join  {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_booking_data as bkg
    ON A.user_id = bkg.user_id
    INNER JOIN {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_offer_data as offer
    ON bkg.offer_id = offer.offer_id
    LEFT JOIN (
        SELECT * except(row_number)
        FROM ( select *, ROW_NUMBER() OVER (PARTITION BY user_id) as row_number
            FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_qpi_answers_v3
        )
    WHERE row_number=1) AS C
    ON A.user_id = C.user_id
    WHERE {where_user_in}
    ORDER BY A.user_id, booking_creation_date"""
    data = pd.read_gbq(query)
    data["user_civility"] = data["user_civility"].replace(["M.", "Mme"], ["M", "F"])
    data["format"] = data.apply(
        lambda x: fuse_columns_into_format(
            x["physical_goods"], x["digital_goods"], x["event"]
        ),
        axis=1,
    )
    return data


def get_rayon():
    data_rayon = pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/macron_rayon/correspondance_rayon_macro_rayon.csv",
        sep=",",
    )
    data_rayon = data_rayon.drop(columns=["Unnamed: 0"])
    return data_rayon


def diversification_kpi(df):
    df_clean = df.rename(columns={"venue_id": "venue"})
    features = ["category", "subcategory", "format", "venue", "macro_rayon"]
    divers_per_feature = calculate_diversification_per_feature(df_clean, features)
    for feature in features:
        divers_col = {f"{feature}_diversification": divers_per_feature[feature]}
        df_clean = pd.merge(
            df_clean, pd.DataFrame(divers_col), left_index=True, right_index=True
        )

    divers_col = {f"qpi_diversification": divers_per_feature["qpi_diversification"]}
    df_clean = pd.merge(
        df_clean, pd.DataFrame(divers_col), left_index=True, right_index=True
    )
    # Calculate delta diversification
    features_qpi = features
    features_qpi.append("qpi")
    df_clean[f"delta_diversification"] = df_clean.apply(
        lambda x: sum(
            [float(x[f"{feature}_diversification"]) for feature in features_qpi]
        ),
        axis=1,
    )
    return df_clean


class DiversificationBatchThread(threading.Thread):
   def __init__(self, threadID, name, q):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.q = q
   def run(self):
      print("Starting " + self.name)
      process_diversification(self.name, self.q)
      print("Exiting " + self.name)


def process_diversification(threadName, q):
   while not exitFlag:
      queueLock.acquire()
         if not workQueue.empty():
            data = q.get()
            queueLock.release()
            print("%s processing %s" % (threadName, data))
         else:
            queueLock.release()
         time.sleep(1)

if __name__ == "__main__":
    count = count_data()
    batch_size = 10000
    # roof division to get number of batches
    batch_number = int(-1 * (-count // batch_size))

    # Timers
    get_data_timer = 0
    merge_rayon_timer = 0
    diversification_timer = 0

    threadList = ["Thread-1", "Thread-2", "Thread-3"]
    batchList = range(batch_number)
    queueLock = threading.Lock()
    workQueue = queue.Queue(10)
    threads = []
    threadID = 1

    # Create new threads
    for tName in threadList:
        thread = DiversificationBatchThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Fill the queue
    queueLock.acquire()
    for word in nameList:
        workQueue.put(word)
    queueLock.release()

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    exitFlag = 1

    # Wait for all threads to complete
    for t in threads:
        t.join()
    print("Exiting Main Thread")

    # calculate diversification in batch of users
    for batch in range(batch_number):
        t0 = time.time()
        df_users = get_batch_of_users(batch, batch_size)
        t1 = time.time()
        print(f"get batch of users : {(t1 - t0)/60} min")

        t0_1 = time.time()
        df = get_data(df_users)
        t0 = time.time()
        get_data_timer += (t0 - t0_1)/60

        t1 = time.time()
        df_macro_rayons = get_rayon()
        data = pd.merge(df, df_macro_rayons, on="rayon", how="left")
        t1_1 = time.time()
        merge_rayon_timer += (t1_1 - t1)/60
        print(f"merge macro rayon : {(t1_1 - t1)/60} min")

        data = data.drop(columns=["submitted_at"])
        data = data.drop(
            columns=[
                "physical_goods",
                "digital_goods",
                "event",
                "genres",
                "rayon",
                "user_total_deposit_amount",
                "actual_amount_spent",
            ]
        )
        data = data.sort_values(by=["user_id", "booking_creation_date"])

        t2 = time.time()
        df = diversification_kpi(data)
        t3 = time.time()
        diversification_timer += (t3 - t2)/60
        print(f"calcul diversification : {(t3-t2)/60} min")

        df = df[
            [
                "user_id",
                "offer_id",
                "booking_id",
                "booking_creation_date",
                "category",
                "subcategory",
                "type",
                "venue",
                "venue_name",
                "user_region_name",
                "user_activity",
                "user_civility",
                "booking_amount",
                "user_deposit_creation_date",
                "format",
                "macro_rayon",
                "category_diversification",
                "subcategory_diversification",
                "format_diversification",
                "venue_diversification",
                "macro_rayon_diversification",
                "qpi_diversification",
                "delta_diversification",
            ]
        ]

        df.to_gbq(
            f"""{BIGQUERY_ANALYTICS_DATASET}.diversification_booking""",
            project_id=f"{GCP_PROJECT}",
            if_exists=("replace" if batch == 0 else "append"),
        )

    print(f"get data : {get_data_timer} min")