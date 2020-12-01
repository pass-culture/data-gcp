def define_simple_query_1(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.simple_table_1 AS (
            SELECT 
                id,
                2 * float_col AS new_float_col,
                SUBSTR(string_col, 2, 2) AS new_string_col
            FROM {dataset}.raw_table
        );
    """


def define_simple_query_2(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.simple_table_2 AS (
            SELECT 
                id,
                3 * float_col AS new_float_col,
                SUBSTR(string_col, 3, 3) AS new_string_col
            FROM {dataset}.raw_table
        );
    """
