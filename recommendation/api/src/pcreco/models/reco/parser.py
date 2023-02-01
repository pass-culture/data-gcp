import re
from pcreco.models.reco.playlist_params import PlaylistParamsIn

under_pat = re.compile(r"_([a-z])")


def underscore_to_camel(name):
    return under_pat.sub(lambda x: x.group(1).upper(), name)


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


def parse_internal(request):
    """Get the internal flag. This flag is used when internal queries are
    sent for test purposes.
    """

    try:
        internal = int(request.args.get("internal", 0)) == 1
    except:
        internal = False
    return internal


def parse_params(request) -> PlaylistParamsIn:

    if request.method == "POST":
        params = dict(request.get_json(), **dict(request.args))

    elif request.method == "GET":
        params = request.args
        print(params)
    if params is None:
        params = {}
    params = {underscore_to_camel(k): v for k, v in params.items()}

    return PlaylistParamsIn(params)
