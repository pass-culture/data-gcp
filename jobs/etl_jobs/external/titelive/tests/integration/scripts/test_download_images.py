"""Tests for download_images script."""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.scripts.download_images import (
    _process_batch_images,
    _process_image_url,
    run_download_images,
)


class TestProcessImageUrl:
    """Tests for _process_image_url function."""

    def test_processes_valid_url(self):
        """Test processing of valid image URL."""
        download_tasks = []
        ean_images = []

        is_placeholder, image_added, uuid = _process_image_url(
            image_url="https://example.com/image.jpg",
            image_type="recto",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
        )

        assert not is_placeholder
        assert image_added
        assert uuid is not None
        assert len(download_tasks) == 1
        assert len(ean_images) == 1
        assert ean_images[0]["type"] == "recto"

    def test_detects_placeholder_no_image_url(self):
        """Test detection of placeholder 'no_image' URLs with caching."""
        download_tasks = []
        ean_images = []
        no_image_cache = {}
        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True  # Placeholder already in bucket

        is_placeholder, image_added, uuid = _process_image_url(
            image_url="https://images.epagine.fr/no_image_musique.png",
            image_type="recto",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
            storage_client=mock_storage_client,
            no_image_cache=no_image_cache,
        )

        assert is_placeholder
        assert not image_added
        assert uuid is not None  # Should return UUID
        assert len(download_tasks) == 0  # No download needed (exists)
        assert len(ean_images) == 0
        # UUID should be cached
        assert "https://images.epagine.fr/no_image_musique.png" in no_image_cache

    def test_detects_placeholder_with_uppercase(self):
        """Test detection of placeholder URLs with uppercase."""
        download_tasks = []
        ean_images = []
        no_image_cache = {}
        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        is_placeholder, image_added, uuid = _process_image_url(
            image_url="https://example.com/NO_IMAGE.png",
            image_type="verso",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
            storage_client=mock_storage_client,
            no_image_cache=no_image_cache,
        )

        assert is_placeholder
        assert not image_added
        assert uuid is not None

    def test_handles_none_url(self):
        """Test handling of None URL."""
        download_tasks = []
        ean_images = []

        is_placeholder, image_added, uuid = _process_image_url(
            image_url=None,
            image_type="recto",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
        )

        assert not is_placeholder
        assert not image_added
        assert uuid is None
        assert len(download_tasks) == 0

    def test_creates_correct_gcs_path(self):
        """Test that GCS path is correctly formatted."""
        download_tasks = []
        ean_images = []

        _process_image_url(
            image_url="https://example.com/image.png",
            image_type="recto",
            gcs_bucket="my-bucket",
            gcs_prefix="my-prefix",
            download_tasks=download_tasks,
            ean_images=ean_images,
        )

        assert len(download_tasks) == 1
        url, gcs_path = download_tasks[0]
        assert gcs_path.startswith("gs://my-bucket/my-prefix/")
        # Verify no extension is appended (UUID only)
        assert not any(
            gcs_path.endswith(ext)
            for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"]
        )

    def test_placeholder_cache_hit(self):
        """Test that cached placeholder UUIDs are returned without GCS calls."""
        download_tasks = []
        ean_images = []
        no_image_cache = {"https://images.epagine.fr/no_image.png": "cached-uuid-123"}
        mock_storage_client = Mock()

        is_placeholder, image_added, uuid = _process_image_url(
            image_url="https://images.epagine.fr/no_image.png",
            image_type="recto",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
            storage_client=mock_storage_client,
            no_image_cache=no_image_cache,
        )

        assert is_placeholder
        assert not image_added
        assert uuid == "cached-uuid-123"
        # No GCS calls should be made (cache hit)
        mock_storage_client.bucket.assert_not_called()
        assert len(download_tasks) == 0

    def test_placeholder_not_in_bucket_downloads(self):
        """Test that missing placeholder images are added to download tasks."""
        download_tasks = []
        ean_images = []
        no_image_cache = {}
        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False  # Placeholder NOT in bucket

        is_placeholder, image_added, uuid = _process_image_url(
            image_url="https://images.epagine.fr/no_image_new.png",
            image_type="recto",
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            download_tasks=download_tasks,
            ean_images=ean_images,
            storage_client=mock_storage_client,
            no_image_cache=no_image_cache,
        )

        assert is_placeholder
        assert not image_added
        assert uuid is not None
        # Should add to download tasks (doesn't exist in bucket)
        assert len(download_tasks) == 1
        assert download_tasks[0][0] == "https://images.epagine.fr/no_image_new.png"
        # UUID should be cached
        assert "https://images.epagine.fr/no_image_new.png" in no_image_cache

    def test_multiple_eans_same_placeholder_uses_cache(self):
        """Test that multiple EANs with same placeholder only check bucket once."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://images.epagine.fr/no_image.png",
                                },
                                "image": 1,
                            }
                        }
                    }
                ),
                "old_recto_image_uuid": None,
                "old_verso_image_uuid": None,
            },
            {
                "ean": "9780987654321",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://images.epagine.fr/no_image.png",
                                },
                                "image": 1,
                            }
                        }
                    }
                ),
                "old_recto_image_uuid": None,
                "old_verso_image_uuid": None,
            },
        ]

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        mock_session = Mock()

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 2
        # Both should have same UUID (from cache)
        assert results[0]["recto_image_uuid"] == results[1]["recto_image_uuid"]
        # Bucket should only be checked once (first EAN), second uses cache
        assert mock_blob.exists.call_count == 1


class TestProcessBatchImages:
    """Tests for _process_batch_images function."""

    @pytest.fixture
    def mock_storage_client(self):
        """Mock storage client."""
        return Mock()

    @pytest.fixture
    def mock_session(self):
        """Mock requests session."""
        return Mock()

    def test_processes_successful_downloads(self, mock_storage_client, mock_session):
        """Test processing with successful downloads."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 1,
                                "image_4": 1,
                            }
                        }
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            # Mock successful downloads
            mock_download.return_value = [
                (True, "https://example.com/recto.jpg", "Success"),
                (True, "https://example.com/verso.jpg", "Success"),
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["ean"] == "9781234567890"
            assert results[0]["images_download_status"] == "processed"
            assert results[0]["images_download_processed_at"] is not None
            assert results[0]["recto_image_uuid"] is not None
            assert results[0]["verso_image_uuid"] is not None

    def test_processes_failed_downloads(self, mock_storage_client, mock_session):
        """Test processing with failed downloads."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 1,
                                "image_4": 1,
                            }
                        }
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            # Mock failed download for recto
            mock_download.return_value = [
                (False, "https://example.com/recto.jpg", "404 Not Found"),
                (True, "https://example.com/verso.jpg", "Success"),
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "failed"

    def test_handles_parsing_errors(self, mock_storage_client, mock_session):
        """Test handling of JSON parsing errors."""
        rows = [{"ean": "9781234567890", "json_raw": "invalid json"}]

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 1
        assert results[0]["images_download_status"] == "failed"

    def test_handles_placeholder_only_eans(self, mock_storage_client, mock_session):
        """Test handling of EANs with only placeholder images."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://images.epagine.fr/no_image.png",
                                    "verso": "https://images.epagine.fr/no_image_musique.png",
                                },
                                "image": 1,
                                "image_4": 1,
                            }
                        }
                    }
                ),
                "old_recto_image_uuid": None,
                "old_verso_image_uuid": None,
            }
        ]

        # Mock GCS blob existence checks
        mock_bucket = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_blob.exists.return_value = True  # Placeholders already exist
        mock_bucket.blob.return_value = mock_blob

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 1
        assert results[0]["images_download_status"] == "processed"
        # Should now have UUIDs for placeholders
        assert results[0]["recto_image_uuid"] is not None
        assert results[0]["verso_image_uuid"] is not None

    def test_handles_mixed_real_and_placeholder(
        self, mock_storage_client, mock_session
    ):
        """Test handling of EANs with mix of real and placeholder images."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://images.epagine.fr/no_image.png",
                                },
                                "image": 1,
                                "image_4": 1,
                            }
                        }
                    }
                ),
                "old_recto_image_uuid": None,
                "old_verso_image_uuid": None,
            }
        ]

        # Mock GCS blob existence checks for placeholder
        mock_bucket = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            mock_download.return_value = [
                (True, "https://example.com/recto.jpg", "Success")
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "processed"
            assert results[0]["recto_image_uuid"] is not None
            assert (
                results[0]["verso_image_uuid"] is not None
            )  # Now has placeholder UUID

    def test_handles_article_list_format(self, mock_storage_client, mock_session):
        """Test handling of article in list format."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": [
                            {
                                "imagesUrl": {"recto": "https://example.com/recto.jpg"},
                                "image": 1,
                            }
                        ]
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            mock_download.return_value = [
                (True, "https://example.com/recto.jpg", "Success")
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "processed"

    def test_handles_no_images(self, mock_storage_client, mock_session):
        """Test handling of EANs with no image URLs."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps({"article": {"1": {"imagesUrl": {}}}}),
            }
        ]

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 1
        assert results[0]["images_download_status"] == "no_change"
        assert results[0]["recto_image_uuid"] is None
        assert results[0]["verso_image_uuid"] is None

    def test_skips_recto_when_image_field_is_zero(
        self, mock_storage_client, mock_session
    ):
        """Test that recto is skipped when image field is 0."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 0,  # Recto not valid
                                "image_4": 1,  # Verso valid
                            }
                        }
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            # Only verso should be downloaded
            mock_download.return_value = [
                (True, "https://example.com/verso.jpg", "Success")
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "processed"
            assert results[0]["recto_image_uuid"] is None  # Skipped
            assert results[0]["verso_image_uuid"] is not None  # Downloaded

    def test_skips_verso_when_image_4_field_is_zero(
        self, mock_storage_client, mock_session
    ):
        """Test that verso is skipped when image_4 field is 0."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 1,  # Recto valid
                                "image_4": 0,  # Verso not valid
                            }
                        }
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            # Only recto should be downloaded
            mock_download.return_value = [
                (True, "https://example.com/recto.jpg", "Success")
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "processed"
            assert results[0]["recto_image_uuid"] is not None  # Downloaded
            assert results[0]["verso_image_uuid"] is None  # Skipped

    def test_skips_both_when_both_fields_are_zero(
        self, mock_storage_client, mock_session
    ):
        """Test that both images are skipped when both fields are 0."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 0,  # Recto not valid
                                "image_4": 0,  # Verso not valid
                            }
                        }
                    }
                ),
            }
        ]

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 1
        assert results[0]["images_download_status"] == "no_change"
        assert results[0]["recto_image_uuid"] is None
        assert results[0]["verso_image_uuid"] is None

    def test_skips_when_fields_missing(self, mock_storage_client, mock_session):
        """Test that images are skipped when validation fields are absent."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                }
                                # No image or image_4 fields
                            }
                        }
                    }
                ),
            }
        ]

        results = _process_batch_images(
            rows=rows,
            storage_client=mock_storage_client,
            session=mock_session,
            gcs_bucket="test-bucket",
            gcs_prefix="images",
            max_workers=4,
            timeout=30,
        )

        assert len(results) == 1
        assert results[0]["images_download_status"] == "no_change"
        assert results[0]["recto_image_uuid"] is None
        assert results[0]["verso_image_uuid"] is None

    def test_processes_when_both_fields_are_one(
        self, mock_storage_client, mock_session
    ):
        """Test that both images are processed when both fields are 1."""
        rows = [
            {
                "ean": "9781234567890",
                "json_raw": json.dumps(
                    {
                        "article": {
                            "1": {
                                "imagesUrl": {
                                    "recto": "https://example.com/recto.jpg",
                                    "verso": "https://example.com/verso.jpg",
                                },
                                "image": 1,  # Recto valid
                                "image_4": 1,  # Verso valid
                            }
                        }
                    }
                ),
            }
        ]

        with patch(
            "src.scripts.download_images.batch_download_and_upload"
        ) as mock_download:
            mock_download.return_value = [
                (True, "https://example.com/recto.jpg", "Success"),
                (True, "https://example.com/verso.jpg", "Success"),
            ]

            results = _process_batch_images(
                rows=rows,
                storage_client=mock_storage_client,
                session=mock_session,
                gcs_bucket="test-bucket",
                gcs_prefix="images",
                max_workers=4,
                timeout=30,
            )

            assert len(results) == 1
            assert results[0]["images_download_status"] == "processed"
            assert results[0]["recto_image_uuid"] is not None
            assert results[0]["verso_image_uuid"] is not None


class TestRunDownloadImages:
    """Tests for run_download_images function."""

    @patch("src.scripts.download_images.storage.Client")
    @patch("src.scripts.download_images.bigquery.Client")
    @patch("src.scripts.download_images._get_session")
    def test_normal_mode_processes_pending(
        self, mock_get_session, mock_bq_class, mock_storage_class
    ):
        """Test run_download_images in normal mode (pending images)."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_storage_client = Mock()
        mock_storage_class.return_value = mock_storage_client
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        with (
            patch(
                "src.scripts.download_images.count_pending_image_downloads"
            ) as mock_count,
            patch(
                "src.scripts.download_images.get_last_batch_number"
            ) as mock_last_batch,
            patch(
                "src.scripts.download_images.fetch_batch_for_image_download"
            ) as mock_fetch,
            patch("src.scripts.download_images._process_batch_images") as mock_process,
            patch(
                "src.scripts.download_images.update_image_download_results"
            ) as mock_update,
            patch("src.scripts.download_images.get_status_breakdown") as mock_status,
            patch(
                "src.scripts.download_images.get_images_status_breakdown"
            ) as mock_img,
            patch(
                "src.scripts.download_images.get_eans_not_in_product_table"
            ) as mock_not_in,
            patch(
                "src.scripts.download_images.get_sample_eans_by_images_status"
            ) as mock_sample,
        ):
            mock_count.return_value = 10
            mock_last_batch.return_value = 0
            mock_fetch.return_value = [{"ean": "123", "json_raw": "{}"}]  # Batch 0
            mock_process.return_value = [
                {
                    "ean": "123",
                    "images_download_status": "processed",
                    "images_download_processed_at": datetime.now(),
                    "recto_image_uuid": "uuid1",
                    "verso_image_uuid": "uuid2",
                }
            ]
            mock_status.return_value = {"processed": 10}
            mock_img.return_value = {"NULL": 10, "processed": 0}
            mock_not_in.return_value = (0, [])
            mock_sample.return_value = []

            run_download_images(reprocess_failed=False)

            # Should count pending images
            mock_count.assert_called_once()
            # Should fetch batch 0 (only batch, since last_batch=0)
            assert mock_fetch.call_count == 1
            # Should process batch
            mock_process.assert_called_once()
            # Should update results
            mock_update.assert_called_once()

    @patch("src.scripts.download_images.storage.Client")
    @patch("src.scripts.download_images.bigquery.Client")
    @patch("src.scripts.download_images._get_session")
    def test_reprocess_failed_mode(
        self, mock_get_session, mock_bq_class, mock_storage_class
    ):
        """Test run_download_images in reprocess failed mode."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_storage_client = Mock()
        mock_storage_class.return_value = mock_storage_client
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        with (
            patch(
                "src.scripts.download_images.count_failed_image_downloads"
            ) as mock_count,
            patch(
                "src.scripts.download_images.get_last_batch_number"
            ) as mock_last_batch,
            patch(
                "src.scripts.download_images.fetch_batch_for_image_download"
            ) as mock_fetch,
            patch("src.scripts.download_images._process_batch_images") as mock_process,
            patch("src.scripts.download_images.update_image_download_results"),
            patch("src.scripts.download_images.get_status_breakdown") as mock_status,
            patch(
                "src.scripts.download_images.get_images_status_breakdown"
            ) as mock_img,
            patch(
                "src.scripts.download_images.get_eans_not_in_product_table"
            ) as mock_not_in,
            patch(
                "src.scripts.download_images.get_sample_eans_by_images_status"
            ) as mock_sample,
        ):
            mock_count.return_value = 5
            mock_last_batch.return_value = 0
            mock_fetch.side_effect = [[{"ean": "123", "json_raw": "{}"}], []]
            mock_process.return_value = [
                {
                    "ean": "123",
                    "images_download_status": "processed",
                    "images_download_processed_at": datetime.now(),
                    "recto_image_uuid": "uuid1",
                    "verso_image_uuid": "uuid2",
                }
            ]
            mock_status.return_value = {"processed": 5}
            mock_img.return_value = {"failed": 5, "processed": 0}
            mock_not_in.return_value = (0, [])
            mock_sample.return_value = []

            run_download_images(reprocess_failed=True)

            # Should count failed images
            mock_count.assert_called_once()
            # Should fetch with reprocess_failed=True
            mock_fetch.assert_called()
            assert mock_fetch.call_args_list[0][0][3] is True  # reprocess_failed arg

    @patch("src.scripts.download_images.storage.Client")
    @patch("src.scripts.download_images.bigquery.Client")
    @patch("src.scripts.download_images._get_session")
    def test_handles_no_pending_images(
        self, mock_get_session, mock_bq_class, mock_storage_class
    ):
        """Test handling when no pending images exist."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_storage_client = Mock()
        mock_storage_class.return_value = mock_storage_client
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        with (
            patch(
                "src.scripts.download_images.count_pending_image_downloads"
            ) as mock_count,
            patch("src.scripts.download_images._process_batch_images") as mock_process,
            patch("src.scripts.download_images.get_status_breakdown") as mock_status,
            patch(
                "src.scripts.download_images.get_images_status_breakdown"
            ) as mock_img,
        ):
            mock_count.return_value = 0
            mock_status.return_value = {"processed": 0}
            mock_img.return_value = {"NULL": 0}

            run_download_images(reprocess_failed=False)

            # Should not process anything
            mock_process.assert_not_called()

    @patch("src.scripts.download_images.storage.Client")
    @patch("src.scripts.download_images.bigquery.Client")
    @patch("src.scripts.download_images._get_session")
    def test_processes_multiple_batches(
        self, mock_get_session, mock_bq_class, mock_storage_class
    ):
        """Test processing multiple batches."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_storage_client = Mock()
        mock_storage_class.return_value = mock_storage_client
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        with (
            patch(
                "src.scripts.download_images.count_pending_image_downloads"
            ) as mock_count,
            patch(
                "src.scripts.download_images.get_last_batch_number"
            ) as mock_last_batch,
            patch(
                "src.scripts.download_images.fetch_batch_for_image_download"
            ) as mock_fetch,
            patch("src.scripts.download_images._process_batch_images") as mock_process,
            patch(
                "src.scripts.download_images.update_image_download_results"
            ) as mock_update,
            patch("src.scripts.download_images.get_status_breakdown") as mock_status,
            patch(
                "src.scripts.download_images.get_images_status_breakdown"
            ) as mock_img,
            patch(
                "src.scripts.download_images.get_eans_not_in_product_table"
            ) as mock_not_in,
            patch(
                "src.scripts.download_images.get_sample_eans_by_images_status"
            ) as mock_sample,
        ):
            mock_count.return_value = 100
            mock_last_batch.return_value = 2  # Process batches 0, 1, 2
            mock_fetch.side_effect = [
                [{"ean": "123", "json_raw": "{}"}],  # Batch 0
                [{"ean": "456", "json_raw": "{}"}],  # Batch 1
                [{"ean": "789", "json_raw": "{}"}],  # Batch 2
                [],  # End
            ]
            mock_process.return_value = [
                {
                    "ean": "123",
                    "images_download_status": "processed",
                    "images_download_processed_at": datetime.now(),
                    "recto_image_uuid": "uuid1",
                    "verso_image_uuid": "uuid2",
                }
            ]
            mock_status.return_value = {"processed": 100}
            mock_img.return_value = {"NULL": 100, "processed": 0}
            mock_not_in.return_value = (0, [])
            mock_sample.return_value = []

            run_download_images(reprocess_failed=False)

            # Should process 3 batches
            assert mock_process.call_count == 3
            assert mock_update.call_count == 3

    @patch("src.scripts.download_images.storage.Client")
    @patch("src.scripts.download_images.bigquery.Client")
    @patch("src.scripts.download_images._get_session")
    def test_processes_sub_batches(
        self, mock_get_session, mock_bq_class, mock_storage_class
    ):
        """Test that large batches are processed in sub-batches."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_storage_client = Mock()
        mock_storage_class.return_value = mock_storage_client
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        with (
            patch(
                "src.scripts.download_images.count_pending_image_downloads"
            ) as mock_count,
            patch(
                "src.scripts.download_images.get_last_batch_number"
            ) as mock_last_batch,
            patch(
                "src.scripts.download_images.fetch_batch_for_image_download"
            ) as mock_fetch,
            patch("src.scripts.download_images._process_batch_images") as mock_process,
            patch("src.scripts.download_images.update_image_download_results"),
            patch("src.scripts.download_images.get_status_breakdown") as mock_status,
            patch(
                "src.scripts.download_images.get_images_status_breakdown"
            ) as mock_img,
            patch(
                "src.scripts.download_images.get_eans_not_in_product_table"
            ) as mock_not_in,
            patch(
                "src.scripts.download_images.get_sample_eans_by_images_status"
            ) as mock_sample,
        ):
            mock_count.return_value = 100
            mock_last_batch.return_value = 0

            # Return 2500 rows
            # (should be split into 3 sub-batches with sub_batch_size=1000)
            large_batch = [{"ean": f"{i}", "json_raw": "{}"} for i in range(2500)]
            mock_fetch.side_effect = [large_batch, []]

            mock_process.return_value = [
                {
                    "ean": "1",
                    "images_download_status": "processed",
                    "images_download_processed_at": datetime.now(),
                    "recto_image_uuid": "uuid1",
                    "verso_image_uuid": "uuid2",
                }
            ]
            mock_status.return_value = {"processed": 100}
            mock_img.return_value = {"NULL": 100, "processed": 0}
            mock_not_in.return_value = (0, [])
            mock_sample.return_value = []

            run_download_images(reprocess_failed=False)

            # Should process in 3 sub-batches (2500 / 1000 = 3)
            assert mock_process.call_count == 3
