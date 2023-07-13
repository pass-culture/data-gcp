
from typing import List, Dict
from pydantic import BaseModel
from datetime import datetime
import sqlalchemy
from src.utils.env_vars import NUMBER_OF_RECOMMENDATIONS, MixingFeatures

from psycopg2 import sql

class PlaylistParamsRequest(BaseModel):
    """Acceptable input in a API request for playlist parameters"""

    model_endpoint: str = "default"
    start_date: datetime = None # beginningDatetime when None ?
    end_date: datetime = None # endingDatetime when None ?
    search_group_names: List[str] = None

    def _get_conditions(self) -> sql.SQL:
        """Generate the SQL condition that filter the recommendable offers table."""
        condition = sql.SQL("")
        if self.start_date:
            # if self.is_event:
            #     column = "stock_beginning_date"
            # else:
            column = "offer_creation_date"
            if self.end_date is not None:
                condition += sql.SQL(
                    """AND ({column} > {start_date} AND {column} < {end_date}) \n"""
                ).format(
                    column=sql.SQL(column),
                    start_date=sql.Literal(self.start_date.isoformat()),
                    end_date=sql.Literal(self.end_date.isoformat()),
                )
            else:
                condition += sql.SQL("""AND ({column} > {start_date}) \n""").format(
                    column=sql.SQL(column),
                    start_date=sql.Literal(self.start_date.isoformat()),
                )
        if self.search_group_names is not None and len(self.search_group_names) > 0:
            # we filter by search_group_name to be iso with contentful categories
            condition += sql.SQL("AND ({search_group_name}) \n").format(
                search_group_name=sql.SQL(" OR ").join(
                    [
                        sql.SQL("search_group_name={}").format(sql.Literal(cat))
                        for cat in self.search_group_names
                    ]
                )
            )