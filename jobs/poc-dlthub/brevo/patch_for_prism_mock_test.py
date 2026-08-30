import copy


def _patch_endpoints_for_mock(endpoints):
    """Patch endpoint configs for Prism mock compatibility.

    Prism ignores query params and always returns static example data, so:
    - Paginators with no total_path loop forever (Prism never returns empty pages).
      We cap them with maximum_offset=0 to fetch only the first page.
    """
    patched = copy.deepcopy(endpoints)
    for resource in patched:
        endpoint = resource.get("endpoint", {})
        if isinstance(endpoint, dict):
            paginator = endpoint.get("paginator", {})
            if isinstance(paginator, dict) and paginator.get("total_path") is None:
                paginator["maximum_offset"] = 0
    return patched
