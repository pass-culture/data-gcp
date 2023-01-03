from datetime import datetime


class BucketFolder:
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    def get_yyyymmdd(self, filename):
        return datetime.strptime(filename.split("_")[-1], "%Y%m%d")

    def get_last_file_name(self, storage_client):

        blobs = storage_client.list_blobs(
            self.bucket_name, prefix=f"{self.folder_name}"
        )

        update_list = []
        for blob in blobs:
            update_dict = {}
            if blob.name != self.folder_name + "/":
                update_dict["name"] = blob.name
                update_dict["date"] = self.get_yyyymmdd(blob.name.replace(".csv", ""))
                update_list.append(update_dict)

        file = max(update_list, key=lambda k: k["date"])
        return file["name"].replace(".csv", "").split("/")[-1]
