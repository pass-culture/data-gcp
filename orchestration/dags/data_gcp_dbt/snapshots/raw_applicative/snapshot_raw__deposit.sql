{% snapshot snapshot_raw__deposit %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                unique_key="id",
                updated_at="dateUpdated",
            )
        )
    }}

    select
        id,
        amount,
        userId,
        source,
        dateCreated,
        expirationDate,
        type,
        cast(dateupdated as timestamp) as dateupdated
    from
        external_query(
            "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
            '''SELECT
            CAST("id" AS varchar(255)) as id
            , "amount"
            , CAST("userId" AS varchar(255)) as userId
            , "source"
            , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateCreated
            , "dateUpdated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as dateUpdated
            , "expirationDate"
            , "type"
        FROM public.deposit
    '''
        )

{% endsnapshot %}
