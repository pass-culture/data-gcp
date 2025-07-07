import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from zenpy import Zenpy
from zenpy.lib.exception import APIException, RecordNotFoundException

from constants import (
    MACRO_ACTIONS_MAPPING,
    MACRO_BASE_COLUMNS,
    TICKET_BASE_COLUMNS,
    TICKET_CUSTOM_FIELDS,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ZendeskAPI:
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initializes the ZendeskAPI client with the given credentials.

        Args:
            credentials (Dict[str, Any]): A dictionary containing Zendesk API credentials.
        """
        try:
            logger.info("Initializing Zendesk client.")
            self.client = Zenpy(**credentials)
            logger.info("Zendesk client initialized successfully.")
        except APIException as e:
            logger.error(f"Failed to initialize Zendesk client: {e}")
            raise ValueError(f"Failed to initialize Zendesk client: {e}")

    def fetch_macros(self) -> List[Dict[str, Any]]:
        """
        Fetches active macros with usage statistics.

        Returns:
            List[Dict[str, Any]]: List of macro dictionaries.
        """
        try:
            logger.info("Fetching active macros.")
            macros = [
                macro.to_dict()
                for macro in self.client.macros(
                    active=True, include="usage_1h,usage_24h,usage_7d,usage_30d"
                )
            ]
            logger.info(f"Fetched {len(macros)} macros.")
            return macros
        except APIException as e:
            logger.error(f"Error fetching macros: {e}")
            raise RuntimeError(f"Error fetching macros: {e}")

    def fetch_tickets(
        self,
        from_date: str,
        to_date: str = None,
        status: Optional[str] = None,
        filter_field: str = "updated_at",
    ) -> List[Dict[str, Any]]:
        """
        Fetches closed tickets updated within the specified date range.

        Args:
            from_date (str): The start date for fetching tickets (YYYY-MM-DD).
            to_date (str, optional): The end date for fetching tickets (YYYY-MM-DD). Defaults to None.

        Returns:
            List[Dict[str, Any]]: List of ticket dictionaries.
        """
        try:
            if to_date:
                logger.info(
                    f"Fetching tickets updated between {from_date} and {to_date}."
                )
                query = f"{filter_field}>={from_date} {filter_field}<={to_date}"
            else:
                logger.info(f"Fetching tickets updated after {from_date}.")
                query = f"{filter_field}>={from_date}"
            if status:
                tickets_generator = self.client.search_export(
                    type="ticket",
                    status=status,
                    sort_order="desc",
                    query=query,
                )
            else:
                tickets_generator = self.client.search_export(
                    type="ticket",
                    sort_order="desc",
                    query=query,
                )

            tickets = [ticket.to_dict() for ticket in tickets_generator]
            logger.info(f"Fetched {len(tickets)} tickets.")
            return tickets

        except APIException as e:
            logger.error(f"Error fetching tickets: {e}")
            raise RuntimeError(f"Error fetching tickets: {e}")

    def fetch_satisfaction_ratings(
        self,
        from_date: str,
        to_date: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetches satisfaction ratings within the specified date range.

        Args:
            from_date (str): The start date for fetching ratings (YYYY-MM-DD).
            to_date (str, optional): The end date for fetching ratings (YYYY-MM-DD). Defaults to None.

        Returns:
            List[Dict[str, Any]]: List of satisfaction rating dictionaries.
        """
        try:
            start_time = datetime.strptime(from_date, "%Y-%m-%d")
            if to_date:
                end_time = datetime.strptime(to_date, "%Y-%m-%d") - timedelta(minutes=1)
            else:
                end_time = datetime.now(datetime.timezone.utc) - timedelta(minutes=1)

            logger.info(
                f"Fetching satisfaction ratings between {from_date} and {to_date or 'now'}."
            )
            ratings = [
                rating.to_dict()
                for rating in self.client.satisfaction_ratings(
                    start_time=start_time,
                    end_time=end_time,
                )
            ]
            logger.info(f"Fetched {len(ratings)} satisfaction ratings.")
            return ratings

        except APIException as e:
            logger.error(f"Error fetching satisfaction ratings: {e}")
            raise RuntimeError(f"Error fetching satisfaction ratings: {e}")

    def create_macro_stat_df(self) -> pd.DataFrame:
        """
        Creates a DataFrame from macro statistics.

        Returns:
            pd.DataFrame: DataFrame containing macro statistics.
        """
        logger.info("Creating macro statistics DataFrame.")
        macros = self.fetch_macros()
        df = pd.DataFrame([self._flatten_macro_data(macro) for macro in macros])
        logger.info(f"Macro statistics DataFrame created with {len(df)} rows.")
        return df

    def create_ticket_stat_df(
        self,
        from_date: str,
        to_date: str = None,
        status: Optional[str] = None,
        filter_field: str = "updated_at",
    ) -> pd.DataFrame:
        """
        Creates a DataFrame from ticket statistics.

        Args:
            from_date (str): The start date for fetching tickets (YYYY-MM-DD).
            to_date (str, optional): The end date for fetching tickets (YYYY-MM-DD). Defaults to None.
            status (str, optional): The status of the tickets to fetch. Defaults to "closed".

        Returns:
            pd.DataFrame: DataFrame containing ticket statistics.
        """
        logger.info("Creating ticket statistics DataFrame.")
        tickets = self.fetch_tickets(from_date, to_date, status, filter_field)
        df = pd.DataFrame([self._flatten_ticket_data(ticket) for ticket in tickets])
        logger.info(f"Ticket statistics DataFrame created with {len(df)} rows.")
        return df

    def create_satisfaction_stat_df(
        self,
        from_date: str,
        to_date: str = None,
    ) -> pd.DataFrame:
        """
        Creates a DataFrame from satisfaction ratings.

        Args:
            from_date (str): The start date for fetching ratings (YYYY-MM-DD).
            to_date (str, optional): The end date for fetching ratings (YYYY-MM-DD). Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing satisfaction ratings.
        """
        logger.info("Creating satisfaction ratings DataFrame.")
        ratings = self.fetch_satisfaction_ratings(from_date, to_date)
        df = pd.DataFrame(ratings)
        logger.info(f"Satisfaction ratings DataFrame created with {len(df)} rows.")
        return df

    def _flatten_ticket_data(self, ticket: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flattens a ticket dictionary into a simplified structure.

        Args:
            ticket (Dict[str, Any]): A ticket dictionary.

        Returns:
            Dict[str, Any]: Flattened ticket dictionary.
        """
        flattened = {key: ticket.get(key) for key in TICKET_BASE_COLUMNS}
        for custom_field in ticket.get("custom_fields", []):
            field_key = TICKET_CUSTOM_FIELDS.get(custom_field["id"])
            if field_key:
                value = custom_field.get("value")
                flattened[field_key] = value
        flattened["user_id"] = self._get_user_id(ticket.get("requester_id"))
        return flattened

    def _get_user_id(self, zendesk_id: str) -> Optional[str]:
        """
        Fetches the user_id from the user dictionary.

        Args:
            zendesk_id (str): The Zendesk user id.

        Returns:
            str: The user_id.
        """
        try:
            user_dict = self.client.users(id=zendesk_id).to_dict()
            user = user_dict.get("user_fields", {}).get("user_id")
            if user is not None:
                return str(user)
            else:
                return None

        except RecordNotFoundException:
            logger.warning(f"User {zendesk_id} not found.")
            return None

    def _flatten_macro_data(self, macro: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flattens a macro dictionary into a simplified structure.

        Args:
            macro (Dict[str, Any]): A macro dictionary.

        Returns:
            Dict[str, Any]: Flattened macro dictionary.
        """
        actions = macro.get("actions", [])
        mapped_actions = self._map_macro_actions(actions)
        base_columns = {col: macro.get(col) for col in MACRO_BASE_COLUMNS}
        return {**base_columns, **mapped_actions}

    @staticmethod
    def _map_macro_actions(actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Maps macro actions to their corresponding field names.

        Args:
            actions (List[Dict[str, Any]]): List of actions containing 'field' and 'value' keys.

        Returns:
            Dict[str, Any]: Mapped field names and their values.
        """
        return {
            MACRO_ACTIONS_MAPPING[action["field"]]: action.get("value")
            for action in actions
            if action.get("field") in MACRO_ACTIONS_MAPPING and action.get("value")
        }
