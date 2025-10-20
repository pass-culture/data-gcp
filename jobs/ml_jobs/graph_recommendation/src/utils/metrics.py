import numpy as np

MAX_DEPTH = 4


def get_depth(gtl_id: str) -> int:
    assert len(gtl_id) == 8, "gtl_id must be 8 characters long"
    trailing_zero_pairs = (len(gtl_id) - len(gtl_id.rstrip("0"))) // 2
    return MAX_DEPTH - trailing_zero_pairs


def gtl_dist(gtl_id1: str, gtl_id2: str) -> float:
    depth1, depth2 = map(get_depth, (gtl_id1, gtl_id2))
    shallowness = min(depth1, depth2)
    if shallowness < MAX_DEPTH:
        return 1 + MAX_DEPTH - shallowness

    # Split into 2-character chunks
    chunks1 = [gtl_id1[i : i + 2] for i in range(0, 8, 2)]
    chunks2 = [gtl_id2[i : i + 2] for i in range(0, 8, 2)]

    for idx, (a, b) in enumerate(zip(chunks1, chunks2, strict=True), start=0):
        if a != b:
            return np.inf if idx == 0 else MAX_DEPTH - idx
    return 1


def get_gtl_score(gtl_id1: str, gtl_id2: str) -> float:
    return 1 / gtl_dist(gtl_id1, gtl_id2)
