import re
from pcreco.models.reco.playlist_params import PlaylistParamsIn

under_pat = re.compile(r"_([a-z])")


def underscore_to_camel(name):
    """
    Parse key into camelCase format
    """
    return under_pat.sub(lambda x: x.group(1).upper(), name)


def key_from_list(input):
    """
    Returns When single element present in list
    Else returns the list
    """
    if isinstance(input, list):
        if len(input) == 1:
            return input[0]
    return input


def parse_geolocation(request):
    def _parse(value):
        if value is not None:
            try:
                return float(value)
            except ValueError:
                pass
        return None

    longitude = _parse(request.args.get("longitude", None))
    latitude = _parse(request.args.get("latitude", None))
    if longitude is not None and latitude is not None:
        geo_located = True
    else:
        geo_located = False
    return longitude, latitude, geo_located


def parse_user(request):
    if "user_id" in request.args:
        return request.args.get("user_id")
    elif "userId" in request.args:
        return request.args.get("userId")
    else:
        return -1


def parse_internal(request):
    """Get the internal flag. This flag is used when internal queries are
    sent for test purposes.
    """

    try:
        internal = int(request.args.get("internal", 0)) == 1
    except:
        internal = False
    return internal


def parse_params(request, geo_located) -> PlaylistParamsIn:

    if request.method == "POST":
        params = dict(request.get_json(), **dict(request.args))
    elif request.method == "GET":
        params = request.args.to_dict(flat=False)
    if params is None:
        params = {}
    params = {underscore_to_camel(k): key_from_list(v) for k, v in params.items()}
    return PlaylistParamsIn(params, geo_located)
