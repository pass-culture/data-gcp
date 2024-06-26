import datetime
import decimal
from typing import Optional, Union

from google.cloud.bigquery.table import Row

JsonType = Optional[
    Union[Row, dict, list, str, float, int, bool, datetime.datetime, decimal.Decimal]
]


def approx_equal(js: JsonType, precision: int) -> bool:
    def _float_to_str_in_json(js):
        if type(js) in (str, bool, datetime.datetime, type(None)):
            return js
        if type(js) in (decimal.Decimal, int, float):
            _f = float(js)
            return f"{_f:.{precision}e}"
        if isinstance(js, list):
            return [_float_to_str_in_json(x) for x in js]
        if isinstance(js, dict):
            if any([not isinstance(key, str) for key in js.keys()]):
                raise TypeError("One of key types is not 'str'.")
            return {key: _float_to_str_in_json(val) for key, val in js.items()}
        if type(js) is Row:
            return {key: _float_to_str_in_json(val) for key, val in js.items()}
        raise TypeError(f"Value of type '{type(js)}' is not a valid json object.")

    return _float_to_str_in_json(js)
