"""Google Drive upload service for PPG reports."""

from pathlib import Path
from typing import Any, Dict, Optional

from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from config import MONTH_NAMES_FR
from utils.verbose_logger import log_print

SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveUploadService:
    """Handles Google Drive folder creation and file uploads."""

    def __init__(self):
        """Initialize Drive API with default credentials (GCE service account)."""
        log_print.debug("🔐 Authenticating with Google Drive API")
        try:
            creds, _ = default(scopes=SCOPES)
            self.service = build("drive", "v3", credentials=creds)
            log_print.debug("✅ Drive API client initialized")
        except Exception:
            log_print.error(
                "❌ Authentication failed. For local development, run:\n"
                "   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform",
                fg="red",
            )
            raise

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
            f"name='{name}' and "
            f"'{parent_id}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"trashed=false"
        )

        try:
            results = (
                self.service.files()
                .list(q=query, spaces="drive", fields="files(id, name)")
                .execute()
            )
            items = results.get("files", [])

            if items:
                log_print.debug(
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
            folder = (
                self.service.files().create(body=file_metadata, fields="id").execute()
            )
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
            File ID if found, None otherwise
        """
        query = f"name='{file_name}' and '{parent_id}' in parents and trashed=false"

        try:
            results = (
                self.service.files()
                .list(q=query, spaces="drive", fields="files(id, name)")
                .execute()
            )
            items = results.get("files", [])

            if items:
                return items[0]["id"]
            return None

        except Exception as e:
            log_print.warning(f"Error checking file existence '{file_name}': {e}")
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
            file = (
                self.service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
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

    def verify_folder_access(self, folder_id: str) -> bool:
        """
        Verify that the folder exists and is accessible.

        Args:
            folder_id: Google Drive folder ID

        Returns:
            True if accessible, False otherwise
        """
        try:
            self.service.files().get(fileId=folder_id, fields="id, name").execute()
            return True
        except Exception as e:
            log_print.error(
                f"❌ Cannot access folder ID '{folder_id}': {e}\n"
                f"   Please verify:\n"
                f"   1. The folder ID is correct\n"
                f"   2. Your account has access to this folder\n"
                f"   3. The folder exists in Google Drive",
                fg="red",
            )
            return False

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

        # Verify root folder access first
        if not self.verify_folder_access(root_folder_id):
            stats["errors"].append(f"Cannot access root folder: {root_folder_id}")
            return stats

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

            # 3. Handle REGIONAL/
            regional_dir = local_base_dir / "REGIONAL"
            if regional_dir.exists() and regional_dir.is_dir():
                log_print.info("📂 Processing REGIONAL reports...", fg="cyan")
                regional_folder_id = self.find_or_create_folder(
                    "REGIONAL", export_folder_id
                )
                stats["folders_created"] += 1

                # Iterate through each region folder (sorted alphabetically)
                region_folders = sorted(
                    [f for f in regional_dir.iterdir() if f.is_dir()]
                )

                for region_folder in region_folders:
                    region_name = region_folder.name
                    log_print.info(f"  📁 Processing region: {region_name}")

                    region_folder_id = self.find_or_create_folder(
                        region_name, regional_folder_id
                    )
                    stats["folders_created"] += 1

                    # Upload all .xlsx files in region folder
                    xlsx_files = sorted(region_folder.glob("*.xlsx"))
                    for xlsx_file in xlsx_files:
                        result = self.upload_file(xlsx_file, region_folder_id)
                        if result:
                            stats["files_uploaded"] += 1
                        else:
                            stats["files_skipped"] += 1

            log_print.info("✅ Upload process completed successfully", fg="green")

        except Exception as e:
            error_msg = f"Upload failed: {e}"
            log_print.error(f"❌ {error_msg}", fg="red")
            stats["errors"].append(error_msg)

        return stats
