SIMPLE_TABLE_1_INPUT = {
    "raw_table": [
        {"id": "0", "float_col": 1.0, "string_col": "some text", "unused_col": "xx"},
        {"id": "1", "float_col": 2.0, "string_col": "other text", "unused_col": None},
    ]
}
SIMPLE_TABLE_1_EXPECTED = [
    {"id": "0", "new_float_col": 2.0, "new_string_col": "om"},
    {"id": "1", "new_float_col": 4.0, "new_string_col": "th"},
]

SIMPLE_TABLE_2_INPUT = {
    "raw_table": [
        {"id": "0", "float_col": 1.0, "string_col": "some text", "unused_col": "xx"},
        {"id": "1", "float_col": 2.0, "string_col": "other text", "unused_col": None},
    ]
}
SIMPLE_TABLE_2_EXPECTED = [
    {"id": "0", "new_float_col": 3.0, "new_string_col": "me "},
    {"id": "1", "new_float_col": 6.0, "new_string_col": "her"},
]
