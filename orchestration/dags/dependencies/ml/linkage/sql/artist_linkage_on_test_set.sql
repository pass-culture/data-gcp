WITH
  artists_on_test_set AS (
  SELECT
    artists_to_link.artist_name,
    artists_to_link.offer_category_id,
    artists_to_link.is_synchronised,
    artists_to_link.artist_type,
    artists_to_link.offer_number,
    artists_to_link.total_booking_count,
    test_set.dataset_name,
    test_set.is_my_artist,
    test_set.irrelevant_data
  FROM
    `{{ bigquery_tmp_dataset }}`.artists_to_link AS artists_to_link
  RIGHT JOIN
    `{{ bigquery_tmp_dataset }}`.test_set test_set
  ON
    artists_to_link.artist_name = test_set.artist_name
    AND artists_to_link.offer_category_id = test_set.offer_category_id
    AND artists_to_link.is_synchronised = test_set.is_synchronised
    AND artists_to_link.artist_type = test_set.artist_type )
SELECT
  artists_on_test_set.artist_name,
  artists_on_test_set.offer_category_id,
  artists_on_test_set.is_synchronised,
  artists_on_test_set.artist_type,
  artists_on_test_set.offer_number,
  artists_on_test_set.total_booking_count,
  artists_on_test_set.dataset_name,
  artists_on_test_set.is_my_artist,
  artists_on_test_set.irrelevant_data,
  linked_artists.cluster_id,
  linked_artists.first_artist
FROM
  artists_on_test_set
LEFT JOIN
  `{{ bigquery_tmp_dataset }}`.matched_artists linked_artists
ON
  artists_on_test_set.artist_name = linked_artists.artist_name
  AND artists_on_test_set.offer_category_id = linked_artists.offer_category_id
  AND artists_on_test_set.is_synchronised = linked_artists.is_synchronised
  AND artists_on_test_set.artist_type = linked_artists.artist_type
ORDER BY
  dataset_name,
  cluster_id