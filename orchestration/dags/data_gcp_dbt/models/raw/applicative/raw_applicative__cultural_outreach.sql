select
    cultural_outreach_id,
    offer_id,
    cultural_outreach_claimed_at,
    cultural_outreach_status
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """
SELECT
    CAST("id" AS varchar(255)) AS cultural_outreach_id,
    CAST("offerId" AS VARCHAR(255)) AS offer_id,
    "claimedDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as cultural_outreach_claimed_at,
    "status" AS cultural_outreach_status
FROM public.cultural_outreach
"""
    )