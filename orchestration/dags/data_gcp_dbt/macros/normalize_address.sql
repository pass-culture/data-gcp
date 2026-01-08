{% macro normalize_address(address, postal_code, city) %}
    {#
    Normalizes an address by:
    - Converting to lowercase
    - Removing postal code and city from the address string (if provided)
    - Removing ", France" suffix
    - Removing standalone postal codes (5 digits)
    - Replacing commas with spaces
    - Collapsing multiple spaces into single space
    - Trimming leading/trailing whitespace

    Note: Uses IFNULL to handle NULL postal_code/city values gracefully.
    When postal_code or city is NULL, we skip that replacement step using IF.
#}
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            if(
                                {{ city }} is not null,
                                regexp_replace(
                                    if(
                                        {{ postal_code }} is not null,
                                        regexp_replace(
                                            lower({{ address }}),
                                            r'\s*'
                                            || lower({{ postal_code }})
                                            || r'\s*',
                                            ' '
                                        ),
                                        lower({{ address }})
                                    ),
                                    r'\s*' || lower({{ city }}) || r'\s*',
                                    ' '
                                ),
                                if(
                                    {{ postal_code }} is not null,
                                    regexp_replace(
                                        lower({{ address }}),
                                        r'\s*' || lower({{ postal_code }}) || r'\s*',
                                        ' '
                                    ),
                                    lower({{ address }})
                                )
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
            ),
            r'^\s+|\s+$',
            ''
        )
    )
{% endmacro %}
