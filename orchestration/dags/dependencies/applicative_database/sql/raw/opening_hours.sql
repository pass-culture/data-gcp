WITH timespan_data AS (
    SELECT
        id,
        unnest(timespan) AS time_range
    FROM
        public.opening_hours
), formatted_times AS (
    SELECT
        id,
        lower(time_range)::int AS start_time,
        upper(time_range)::int AS end_time,
        to_char((lower(time_range) / 60)::int, 'FM00') || ':' || to_char((lower(time_range) % 60)::int, 'FM00') || '-' ||
        to_char((upper(time_range) / 60)::int, 'FM00') || ':' || to_char((upper(time_range) % 60)::int, 'FM00') AS readable_time
    FROM
        timespan_data
), aggregated_times AS (
    SELECT
        id,
        string_agg(
            CASE
                WHEN start_time < 720 THEN readable_time
                ELSE NULL
            END, ', '
        ) AS morning_opening_hours,
        string_agg(
            CASE
                WHEN start_time >= 720 THEN readable_time
                ELSE NULL
            END, ', '
        ) AS afternoon_opening_hours
    FROM
        formatted_times
    GROUP BY
        id
)
SELECT
    CAST(oh.id AS varchar(255)) AS opening_hours_id,
    CAST(oh."venueId" AS varchar(255)) AS venue_id,
    CAST(oh."weekday" AS varchar(255)) AS day_of_the_week,
    at.morning_opening_hours AS morning_opening_hours,
    at.afternoon_opening_hours AS afternoon_opening_hours
FROM
    public.opening_hours oh
LEFT JOIN
    aggregated_times at ON oh.id = at.id
