import csv
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List, Optional, Sequence, TextIO, Union

import requests
from requests.exceptions import RequestException

from utils.constant import GEOPF_API_URL as API_URL


@dataclass
class GeocodingConfig:
    """Configuration for geocoding operations."""

    citycode: Optional[str] = None
    postcode: Optional[str] = None
    dialect: str = "unix"


class AddressGeocoder:
    """
    A class to handle address geocoding operations using the API Adresse data.gouv.fr service.

    This class provides methods to geocode addresses in batch using the CSV endpoint,
    handling the conversion between different data formats and API communication.
    """

    def __init__(
        self,
        api_url: str = API_URL,
        config: Optional[GeocodingConfig] = None,
    ):
        """
        Initialize the AddressGeocoder.

        Args:
            api_url: The URL of the geocoding API endpoint
            config: Optional configuration for geocoding operations
        """
        self.api_url = api_url
        self.config = config or GeocodingConfig()

    def _parse_csv_to_records(
        self, filelike: Union[str, TextIO], **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Parse CSV data into a list of dictionary records.

        Args:
            filelike: A string or file-like object containing CSV data
            **kwargs: Additional arguments to pass to csv.DictReader

        Returns:
            List of dictionaries where each dictionary represents a row

        Raises:
            csv.Error: If CSV parsing fails
        """
        try:
            reader = csv.DictReader(filelike, **kwargs)
            return [dict(row) for row in reader]
        except csv.Error as e:
            raise csv.Error(f"Failed to parse CSV data: {str(e)}")

    def _serialize_records_to_csv(
        self, records: List[Dict[str, Any]], **kwargs
    ) -> StringIO:
        """
        Serialize a list of records into CSV format.

        Args:
            records: List of dictionaries to convert to CSV
            **kwargs: Additional arguments to pass to csv.DictWriter

        Returns:
            StringIO object containing the CSV data

        Raises:
            ValueError: If records list is empty or has inconsistent keys
        """
        if not records:
            raise ValueError("Cannot serialize empty list of records to CSV")

        # Validate that all records have the same keys
        first_record_keys = set(records[0].keys())
        for i, record in enumerate(records[1:], 1):
            if set(record.keys()) != first_record_keys:
                raise ValueError(f"Record at index {i} has inconsistent keys")

        filelike = StringIO()
        fieldnames = list(first_record_keys)
        writer = csv.DictWriter(filelike, fieldnames=fieldnames, **kwargs)
        writer.writeheader()
        for record in records:
            writer.writerow(record)
        filelike.seek(0)
        return filelike

    def _prepare_api_payload(self, address_columns: Sequence[str]) -> Dict[str, Any]:
        """
        Prepare the payload for the geocoding API request.

        Args:
            address_columns: Sequence of column names to use for geocoding

        Returns:
            Dictionary containing the API request payload
        """
        payload = {"columns": address_columns}

        if self.config.citycode:
            payload["citycode"] = self.config.citycode
        if self.config.postcode:
            payload["postcode"] = self.config.postcode

        return payload

    def _send_to_api(
        self, csv_data: Union[str, TextIO], payload: Dict[str, Any]
    ) -> requests.Response:
        """
        Send data to the geocoding API.

        Args:
            csv_data: CSV data as string or file-like object
            payload: API request payload

        Returns:
            Response object from the API

        Raises:
            RequestException: If the API request fails
        """
        files = {"data": csv_data}

        try:
            response = requests.post(self.api_url, data=payload, files=files)
            response.raise_for_status()
            return response
        except RequestException as e:
            raise RequestException(f"Failed to geocode addresses: {str(e)}")

    def geocode_batch(
        self, addresses: List[Dict[str, Any]], address_columns: Sequence[str]
    ) -> List[Dict[str, Any]]:
        """
        Geocode a batch of addresses using the /search/csv endpoint.

        Args:
            addresses: List of dictionaries containing address data
            address_columns: Sequence of column names to use for geocoding

        Returns:
            List of dictionaries containing geocoded results

        Raises:
            ValueError: If address_columns is empty or invalid
            RequestException: If the API request fails
        """
        if not address_columns:
            raise ValueError("address_columns cannot be empty")

        if not all(isinstance(col, str) for col in address_columns):
            raise ValueError("All address_columns must be strings")

        csvfile = self._serialize_records_to_csv(addresses, dialect=self.config.dialect)
        payload = self._prepare_api_payload(address_columns)
        response = self._send_to_api(csvfile, payload)

        return self._parse_csv_to_records(
            StringIO(response.text), dialect=self.config.dialect
        )
