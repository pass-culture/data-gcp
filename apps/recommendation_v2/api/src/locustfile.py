from locust import HttpUser, task, between

token_dev = "N7qFKDgVcooTwHUk3osKcLq6co"
# token_stg = "7t2eBnco2G6hsdVMXz4MGGPY4s"
# token_prod = "zC6uDY7iuucaxiNs7bicxTkYmG"


class PcRecoUser(HttpUser):
    wait_time = between(0.5, 2.5)

    # @task
    # def recommendations_Algo_NoQPI(self):
    #     user_id = "1156" #dev
    #     # user_id = "2295432"  # stg
    #     self.client.get(
    #         f"/playlist_recommendation/{user_id}",
    #         params={
    #             "longitude": 2.426287839596142,
    #             "latitude": 48.81049973670181,
    #         },
    #     )

    # @task
    # def recommendations_CS_NoQPI(self):
    #     user_id = "1156" #dev
    #     # user_id = "4320901"  # stg
    #     self.client.get(
    #         f"/playlist_recommendation/{user_id}",
    #         params={
    #             "longitude": 2.426287839596142,
    #             "latitude": 48.81049973670181,
    #         },
    #     )

    @task
    def recommendations_CS_model_wQPI(self):
        user_id = "16505"  # dev
        # user_id = "2890501"  # stg
        self.client.post(
            f"/playlist_recommendation/{user_id}",
            params={
                "token": token_dev,
                "longitude": 2.426287839596142,
                "latitude": 48.81049973670181,
            },
            json={"modelEndpoint": "cold_start_b"},
        )

    # @task
    # def recommendations_Algo_smalltown(self):
    #     # user_id = "1156" #dev
    #     user_id = "1724525"  # stg
    #     self.client.get(
    #         f"/playlist_recommendation/{user_id}",
    #         params={"token": token_dev, "longitude": 1.53333, "latitude": 45.1589},
    #     )

    # @task
    # def recommendations_similar_offers_bigtown(self):
    #     # user_id = "1156" #dev
    #     offer_id = "10000045"  # stg
    #     self.client.get(
    #         f"/similar_offers/{offer_id}",
    #         params={
    #             "token": token_dev,
    #             "longitude": 2.426287839596142,
    #             "latitude": 48.81049973670181,
    #         },
    #     )
