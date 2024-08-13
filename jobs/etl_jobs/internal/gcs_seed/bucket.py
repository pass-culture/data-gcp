from datetime import datetime
import re


class BucketFolder:
    def __init__(self, bucket_name, folder_name):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

    @staticmethod
    def replace_after_last_dot(blob_name: str) -> str:
        """
        Replaces everything after the last dot in a string.

        Parameters:
        blob_name (str): The input string containing the blob name.

        Returns:
        str: The modified string with everything after the last dot removed.
        """
        return re.sub(r"\.[^.]+$", "", blob_name)

    def get_yyyymmdd(self, filename):
        """
        Extracts a date in YYYYMMDD format from the filename and converts it to a datetime object.

        The function assumes that the filename contains a date in the format YYYYMMDD
        at the end, separated by an underscore ('_').

        Parameters:
        filename (str): The name of the file, which contains a date at the end in YYYYMMDD format.

        Returns:
        datetime: A datetime object representing the extracted date.

        Raises:
        ValueError: If the date format in the filename does not match YYYYMMDD.
        """
        try:
            return datetime.strptime(filename.split("_")[-1], "%Y%m%d")
        except ValueError as e:
            raise ValueError(
                f"Filename does not contain a valid date in YYYYMMDD format: {filename}"
            ) from e

    def get_last_file_name(self, storage_client):
        """
        Retrieves the name of the most recent file based on the date embedded in the filename.

        This function lists all blobs (files) in a specified Google Cloud Storage bucket and folder,
        extracts the dates from the filenames, and returns the name of the file with the latest date.

        It assumes filenames contain a date in YYYYMMDD format, which is extracted and compared.

        Parameters:
        storage_client: A client object for interacting with Google Cloud Storage.

        Returns:
        str: The name of the most recent file, without the folder path and file extension.

        Raises:
        ValueError: If no valid files are found in the specified bucket and folder.
        """

        blobs = storage_client.list_blobs(
            self.bucket_name, prefix=f"{self.folder_name}/"
        )

        update_list = []
        for blob in blobs:
            update_dict = {}
            if blob.name != self.folder_name + "/":
                update_dict["name"] = blob.name
                update_dict["date"] = self.get_yyyymmdd(
                    self.replace_after_last_dot(blob.name)
                )
                update_list.append(update_dict)
        if len(update_list) == 0:
            raise ValueError("No valid files found in the specified bucket and folder.")

        file = max(update_list, key=lambda k: k["date"])
        return self.replace_after_last_dot(file["name"]).split("/")[-1]
