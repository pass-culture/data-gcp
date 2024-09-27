import traceback
from typing import Dict

from docarray import Document

from app.logger import logger
from app.retrieval.client import DefaultClient


def handle_exception(
    e: Exception, call_id: str, selected_params: Dict, size: int
) -> Dict:
    """Handle exceptions and log errors."""
    tb = traceback.format_exc()
    logger.error(
        "error",
        extra={
            "uuid": call_id,
            "params": selected_params,
            "size": size,
            "content": {"error": e.__class__.__name__, "trace": tb},
        },
    )
    return {"predictions": []}


def search_by_tops(
    model: DefaultClient,
    selected_params: Dict,
    size: int,
    debug: bool,
    prefilter: bool,
    call_id: str,
    vector_column_name: str,
    re_rank: bool = False,
    user_id: str = None,
) -> Dict:
    """Filter predictions using the model."""
    try:
        results = model.search_by_tops(
            query_filter=selected_params,
            details=debug,
            n=size,
            prefilter=prefilter,
            vector_column_name=vector_column_name,
            re_rank=re_rank,
            user_id=user_id,
        )
        return {"predictions": results}
    except Exception as e:
        return handle_exception(e, call_id, selected_params, size)


def search_by_vector(
    model: DefaultClient,
    vector: Document,
    size: int,
    selected_params: Dict,
    debug: bool,
    call_id: str,
    prefilter: bool,
    vector_column_name: str,
    similarity_metric: str,
    item_id: str = None,
    re_rank: bool = False,
    user_id: str = None,
) -> Dict:
    """Search predictions using a vector."""
    try:
        results = model.search_by_vector(
            vector=vector,
            similarity_metric=similarity_metric,
            n=size,
            query_filter=selected_params,
            details=debug,
            item_id=item_id,
            prefilter=prefilter,
            vector_column_name=vector_column_name,
            re_rank=re_rank,
            user_id=user_id,
        )
        return {"predictions": results}
    except Exception as e:
        return handle_exception(e, call_id, selected_params, size)
