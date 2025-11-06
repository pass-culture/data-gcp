"""Google Drive upload service for PPG reports."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from config import MONTH_NAMES_FR
from utils.verbose_logger import log_print

SCOPES = ["https://www.googleapis.com/auth/drive"]

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[..., T], max_retries: int = 3, initial_delay: float = 1.0
) -> Callable[..., T]:
    """
    Decorator to retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds

    Returns:
        Wrapped function with retry logic
    """

    def wrapper(*args, **kwargs) -> T:
        delay = initial_delay
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except (HttpError, TimeoutError, ConnectionError, OSError) as e:
                last_exception = e
                if attempt < max_retries:
                    log_print.debug(
                        f"Retry {attempt + 1}/{max_retries} after {delay}s: {e}"
                    )
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    log_print.error(f"Max retries reached: {e}", fg="red")

        raise last_exception

    return wrapper


class DriveUploadService:
    """Handles Google Drive folder creation and file uploads."""

    def __init__(self):
        """Initialize Drive API with default credentials (GCE service account)."""
        log_print.info("🔐 Authenticating with Google Drive API")
        try:
            # Get credentials once and reuse for per-thread service instances
            self.creds, _ = default(scopes=SCOPES)
            self.stats_lock = threading.Lock()
            log_print.info("✅ Drive API credentials initialized")
        except Exception:
            log_print.error("❌ Authentication failed", fg="red")
            raise

    def _get_drive_service(self):
        """
        Create a new Drive API service instance for the current thread.

        Returns:
            Google Drive API service instance
        """
        return build("drive", "v3", credentials=self.creds)

    def find_folder(self, name: str, parent_id: str) -> Optional[str]:
        """
        Search for folder by name within parent folder.

        Args:
            name: Folder name to search for
            parent_id: Parent folder ID

        Returns:
            Folder ID if found, None otherwise
        """
        query = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"'{parent_id}' in parents and "
            f"name='{name}' and "
            f"trashed=false"
        )

        try:
            service = self._get_drive_service()

            def _execute_query():
                return (
                    service.files()
                    .list(
                        q=query,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        fields="files(id, name)",
                    )
                    .execute()
                )

            results = retry_with_backoff(_execute_query)()
            items = results.get("files", [])

            if items:
                log_print.info(
                    f"📁 Found existing folder: {name} (ID: {items[0]['id']})"
                )
                return items[0]["id"]
            return None

        except Exception as e:
            log_print.warning(f"Error searching for folder '{name}': {e}")
            return None

    def create_folder(self, name: str, parent_id: str) -> str:
        """
        Create a new folder in Google Drive.

        Args:
            name: Folder name
            parent_id: Parent folder ID

        Returns:
            Created folder ID
        """
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }

        try:
            service = self._get_drive_service()

            def _create_folder():
                return (
                    service.files()
                    .create(body=file_metadata, fields="id", supportsAllDrives=True)
                    .execute()
                )

            folder = retry_with_backoff(_create_folder)()
            folder_id = folder.get("id")
            log_print.info(f"📁 Created folder: {name}")
            return folder_id

        except Exception as e:
            log_print.error(f"Failed to create folder '{name}': {e}", fg="red")
            raise

    def find_or_create_folder(self, name: str, parent_id: str) -> str:
        """
        Find existing folder or create new one.

        Args:
            name: Folder name
            parent_id: Parent folder ID

        Returns:
            Folder ID (existing or newly created)
        """
        folder_id = self.find_folder(name, parent_id)
        if folder_id:
            return folder_id
        return self.create_folder(name, parent_id)

    def file_exists(self, file_name: str, parent_id: str) -> Optional[str]:
        """
        Check if file exists in parent folder.

        Args:
            file_name: File name to search for
            parent_id: Parent folder ID

        Returns:
            File ID if found, None otherwise (or if check fails)
        """
        query = f"name='{file_name}' and '{parent_id}' in parents and trashed=false"

        try:
            service = self._get_drive_service()

            def _check_exists():
                return (
                    service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields="files(id, name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute()
                )

            results = retry_with_backoff(_check_exists)()
            items = results.get("files", [])

            if items:
                return items[0]["id"]
            return None

        except Exception as e:
            log_print.info(
                f"ℹ️  Could not check if '{file_name}' exists after retries: {e}. "
                f"Continuing with upload attempt...",
                fg="yellow",
            )
            return None

    def upload_file(self, local_path: Path, parent_folder_id: str) -> Optional[str]:
        """
        Upload Excel file to Drive folder.

        Args:
            local_path: Local file path
            parent_folder_id: Parent folder ID in Drive

        Returns:
            Uploaded file ID, or None if skipped/failed
        """
        file_name = local_path.name

        # Check if file already exists
        existing_file_id = self.file_exists(file_name, parent_folder_id)
        if existing_file_id:
            log_print.debug(f"⏭️  Skipped (exists): {file_name}")
            return None

        # Upload new file
        file_metadata = {"name": file_name, "parents": [parent_folder_id]}

        media = MediaFileUpload(
            str(local_path),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=True,
        )

        try:
            service = self._get_drive_service()

            def _upload_file():
                return (
                    service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        fields="id",
                        supportsAllDrives=True,
                    )
                    .execute()
                )

            file = retry_with_backoff(_upload_file)()
            file_id = file.get("id")
            log_print.info(f"📤 Uploaded: {file_name}")
            return file_id

        except Exception as e:
            log_print.error(f"Failed to upload '{file_name}': {e}", fg="red")
            return None

    def _format_export_folder_name(self, ds: str) -> str:
        """
        Convert consolidation date to Drive folder name.

        Args:
            ds: Date string in YYYY-MM-DD format

        Returns:
            Formatted folder name like "Export DRAC - Mars 2024"
        """
        year = ds[:4]
        month_num = int(ds[5:7])
        month_name = MONTH_NAMES_FR[month_num]
        return f"Export DRAC - {month_name} {year}"

    def _create_region_folder(
        self, region_name: str, regional_folder_id: str, stats: Dict[str, Any]
    ) -> Optional[str]:
        """
        Create a region folder in Drive.

        Args:
            region_name: Region name
            regional_folder_id: Parent regional folder ID in Drive
            stats: Shared stats dictionary (thread-safe updates)

        Returns:
            Created region folder ID, or None on error
        """
        try:
            log_print.info(f"  📁 Creating folder: {region_name}")
            region_folder_id = self.find_or_create_folder(
                region_name, regional_folder_id
            )
            with self.stats_lock:
                stats["folders_created"] += 1
            return region_folder_id

        except Exception as e:
            error_msg = f"Failed to create folder for region {region_name}: {e}"
            log_print.error(f"❌ {error_msg}", fg="red")
            with self.stats_lock:
                stats["errors"].append(error_msg)
            return None

    def _upload_file_task(
        self, local_path: Path, parent_folder_id: str, stats: Dict[str, Any]
    ) -> None:
        """
        Upload a single file and update stats (thread-safe).

        Args:
            local_path: Local file path
            parent_folder_id: Parent folder ID in Drive
            stats: Shared stats dictionary (thread-safe updates)
        """
        try:
            result = self.upload_file(local_path, parent_folder_id)
            with self.stats_lock:
                if result:
                    stats["files_uploaded"] += 1
                else:
                    stats["files_skipped"] += 1

        except Exception as e:
            error_msg = f"Failed to upload {local_path.name}: {e}"
            log_print.error(f"❌ {error_msg}", fg="red")
            with self.stats_lock:
                stats["errors"].append(error_msg)

    def upload_reports_directory(
        self, local_base_dir: Path, ds: str, root_folder_id: str
    ) -> Dict[str, Any]:
        """
        Upload entire reports directory to Google Drive.

        Args:
            local_base_dir: Local reports directory (e.g., reports_20240301/)
            ds: Consolidation date in YYYY-MM-DD format
            root_folder_id: Google Drive root folder ID

        Returns:
            Statistics dictionary with upload results
        """
        stats = {
            "files_uploaded": 0,
            "folders_created": 0,
            "files_skipped": 0,
            "errors": [],
        }

        try:
            # 1. Create "Export DRAC - Mars 2024" folder
            export_folder_name = self._format_export_folder_name(ds)
            log_print.info(
                f"📁 Creating export folder: {export_folder_name}", fg="cyan"
            )
            export_folder_id = self.find_or_create_folder(
                export_folder_name, root_folder_id
            )
            stats["folders_created"] += 1

            # 2. Handle NATIONAL/
            national_dir = local_base_dir / "NATIONAL"
            if national_dir.exists() and national_dir.is_dir():
                log_print.info("📂 Processing NATIONAL reports...", fg="cyan")
                national_folder_id = self.find_or_create_folder(
                    "NATIONAL", export_folder_id
                )
                stats["folders_created"] += 1

                rapport_national = national_dir / "rapport_national.xlsx"
                if rapport_national.exists():
                    result = self.upload_file(rapport_national, national_folder_id)
                    if result:
                        stats["files_uploaded"] += 1
                    else:
                        stats["files_skipped"] += 1

            # 3. Handle REGIONAL/ (two-phase parallel processing)
            regional_dir = local_base_dir / "REGIONAL"
            if regional_dir.exists() and regional_dir.is_dir():
                log_print.info("📂 Processing REGIONAL reports...", fg="cyan")
                regional_folder_id = self.find_or_create_folder(
                    "REGIONAL", export_folder_id
                )
                stats["folders_created"] += 1

                # Get all region folders
                region_folders = sorted(
                    [f for f in regional_dir.iterdir() if f.is_dir()]
                )

                # PHASE 1: Create all region folders in parallel
                log_print.info(
                    f"  📁 Creating {len(region_folders)} region folders in parallel...",
                    fg="cyan",
                )
                region_folder_map = {}
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_region = {
                        executor.submit(
                            self._create_region_folder,
                            region_folder.name,
                            regional_folder_id,
                            stats,
                        ): region_folder
                        for region_folder in region_folders
                    }
                    for future in as_completed(future_to_region):
                        region_folder = future_to_region[future]
                        try:
                            region_folder_id = future.result()
                            if region_folder_id:
                                region_folder_map[region_folder] = region_folder_id
                        except Exception as e:
                            log_print.error(
                                f"  ❌ Error creating folder: {e}", fg="red"
                            )

                # PHASE 2: Collect all file upload tasks
                log_print.info("  📤 Collecting files to upload...", fg="cyan")
                upload_tasks = []
                for region_folder, region_folder_id in region_folder_map.items():
                    xlsx_files = sorted(region_folder.glob("*.xlsx"))
                    for xlsx_file in xlsx_files:
                        upload_tasks.append((xlsx_file, region_folder_id))

                # PHASE 3: Upload all files in parallel
                log_print.info(
                    f"  🚀 Uploading {len(upload_tasks)} files in parallel...",
                    fg="cyan",
                )
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [
                        executor.submit(
                            self._upload_file_task, file_path, folder_id, stats
                        )
                        for file_path, folder_id in upload_tasks
                    ]
                    # Wait for all uploads to complete
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            log_print.error(f"  ❌ Upload error: {e}", fg="red")

            log_print.info("✅ Upload process completed successfully", fg="green")

        except Exception as e:
            error_msg = f"Upload failed: {e}"
            log_print.error(f"❌ {error_msg}", fg="red")
            stats["errors"].append(error_msg)

        return stats
