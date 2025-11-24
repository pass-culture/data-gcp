{% macro normalize_address(address, postal_code, city) %}
    {#
    Normalizes an address by:
    - Converting to lowercase
    - Removing postal code and city from the address string
    - Removing ", France" suffix
    - Removing standalone postal codes (5 digits)
    - Replacing commas with spaces
    - Collapsing multiple spaces into single space
    - Trimming leading/trailing whitespace
#}
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                lower({{ address }}),
                                r'\s*' || lower({{ postal_code }}) || r'\s*',
                                ' '
                            ),
                            r'\s*' || lower({{ city }}) || r'\s*',
                            ' '
                        ),
                        r',\s*france\s*$',
                        ''
                    ),
                    r'\d{5}',
                    ' '
                ),
                r',\s*',
                ' '
            ),
            r'\s+',
            ' '
        )
    )
{% endmacro %}
