from typing import List
from huggy.schemas.offer import Offer


def sort_offers(
    order_query: dict = {"user_distance": "ASC", "item_score": "ASC"},
    list_offers: List[Offer] = None,
):

    order_statement_list = []
    for column, order in order_query.items():
        if order == "ASC":
            order_statement_list.append(f"offer.{column}")
        elif order == "DESC":
            order_statement_list.append(f"-offer.{column}")

    if len(order_statement_list) > 0:
        function = lambda offer: eval((", ".join(order_statement_list)))
        result = sorted(list_offers, key=function)
        return result
    else:
        return list_offers


def limit_offers(offer_limit: int = 20, list_offers: List[Offer] = None):
    return list_offers[:offer_limit]
