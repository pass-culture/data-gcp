{#
    Start date of a Firebase session, read from its ga_session_id.

    ga_session_id is the session start time in epoch seconds, set once by the SDK, so
    every event of a session gives the same date whenever it is received. Session
    tables are partitioned on it: an incremental run that keeps only the sessions
    starting in the partitions it rewrites can never leave a second copy of a session
    in another partition (sessions crossing midnight, events sent days later under an
    old session id).

    fallback is used when the id is missing or is not a valid timestamp.
#}
{% macro firebase_session_start_date(session_id, fallback, timezone="Europe/Paris") %}
    coalesce(
        date(safe.timestamp_seconds({{ session_id }}), "{{ timezone }}"), {{ fallback }}
    )
{% endmacro %}
