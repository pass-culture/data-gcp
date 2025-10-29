#!/usr/bin/env python3
"""
Upload knowledge text files to Wren AI as instructions.

This script reads all .txt files from a directory and uploads them
to a Wren AI instance via the Knowledge Instructions API.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import requests


class WrenUploader:
    """Upload text files to Wren AI as knowledge instructions."""

    def __init__(
        self,
        base_url: str = "http://localhost:3001",
        api_key: Optional[str] = None,
        is_global: bool = True,
        dry_run: bool = False,
    ):
        """
        Initialize the Wren uploader.

        Args:
            base_url: Base URL of Wren AI instance
            api_key: Optional API key for authentication
            is_global: Whether to create global instructions (vs question-matching)
            dry_run: If True, preview requests without uploading
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.is_global = is_global
        self.dry_run = dry_run
        self.api_endpoint = f"{self.base_url}/api/v1/knowledge/instructions"

        # Statistics
        self.stats = {
            "uploaded": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
        }
        self.errors: List[Dict[str, str]] = []

    def get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def upload_instruction(self, content: str, filename: str) -> bool:
        """
        Upload a single instruction to Wren AI.

        Args:
            content: Instruction text content
            filename: Source filename (for logging)

        Returns:
            True if successful, False otherwise
        """
        payload = {
            "instruction": content,
            "isGlobal": self.is_global,
        }

        if self.dry_run:
            print(f"  [DRY RUN] Would upload: {filename}")
            print(f"  Content length: {len(content)} chars")
            print(f"  Payload: {json.dumps(payload, indent=2)[:200]}...")
            return True

        try:
            response = requests.post(
                self.api_endpoint,
                headers=self.get_headers(),
                json=payload,
                timeout=30,
            )

            if response.status_code == 201:
                result = response.json()
                instruction_id = result.get("id", "unknown")
                print(f"  ✓ Uploaded: {filename} (ID: {instruction_id})")
                return True
            else:
                error_msg = f"HTTP {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", error_msg)
                except Exception:
                    pass

                print(f"  ✗ Failed: {filename} - {error_msg}")
                self.errors.append({"file": filename, "error": error_msg})
                return False

        except requests.exceptions.Timeout:
            print(f"  ✗ Failed: {filename} - Request timeout")
            self.errors.append({"file": filename, "error": "Request timeout"})
            return False
        except requests.exceptions.ConnectionError:
            print(f"  ✗ Failed: {filename} - Connection error (is Wren AI running?)")
            self.errors.append(
                {"file": filename, "error": "Connection error - is Wren AI running?"}
            )
            return False
        except Exception as e:
            print(f"  ✗ Failed: {filename} - {str(e)}")
            self.errors.append({"file": filename, "error": str(e)})
            return False

    def upload_directory(self, input_dir: Path) -> None:
        """
        Upload all .txt files from a directory.

        Args:
            input_dir: Directory containing text files
        """
        if not input_dir.exists():
            print(f"❌ Directory not found: {input_dir}")
            sys.exit(1)

        # Get all .txt files
        txt_files = sorted(input_dir.glob("*.txt"))

        if not txt_files:
            print(f"⚠️  No .txt files found in {input_dir}")
            sys.exit(0)

        self.stats["total"] = len(txt_files)

        print(f"\n{'='*60}")
        print("Wren AI Knowledge Uploader")
        print(f"{'='*60}")
        print(f"Input directory: {input_dir}")
        print(f"Wren AI URL: {self.base_url}")
        print(
            f"Instruction type: {'Global' if self.is_global else 'Question-matching'}"
        )
        print(f"Files to upload: {self.stats['total']}")
        if self.dry_run:
            print("Mode: DRY RUN (no actual uploads)")
        print(f"{'='*60}\n")

        # Check connectivity before starting (skip in dry-run mode)
        if not self.dry_run:
            try:
                health_check = requests.get(f"{self.base_url}/api/v1/health", timeout=5)
                if health_check.status_code != 200:
                    print(
                        f"⚠️  Warning: Wren AI health check returned status {health_check.status_code}"
                    )
            except requests.exceptions.ConnectionError:
                print(f"❌ Cannot connect to Wren AI at {self.base_url}")
                print("   Make sure Wren AI is running on port 3000")
                sys.exit(1)
            except Exception as e:
                print(f"⚠️  Warning: Health check failed: {e}")

        # Upload each file
        for idx, txt_file in enumerate(txt_files, 1):
            print(f"[{idx}/{self.stats['total']}] Processing: {txt_file.name}")

            try:
                content = txt_file.read_text(encoding="utf-8")

                if not content.strip():
                    print(f"  ⊘ Skipped: {txt_file.name} (empty file)")
                    self.stats["skipped"] += 1
                    continue

                success = self.upload_instruction(content, txt_file.name)

                if success:
                    self.stats["uploaded"] += 1
                else:
                    self.stats["failed"] += 1

            except Exception as e:
                print(f"  ✗ Error reading file: {txt_file.name} - {e}")
                self.stats["failed"] += 1
                self.errors.append({"file": txt_file.name, "error": f"Read error: {e}"})

        # Print summary
        self.print_summary()

    def print_summary(self) -> None:
        """Print upload summary statistics."""
        print(f"\n{'='*60}")
        print("Upload Summary")
        print(f"{'='*60}")
        print(f"Total files:    {self.stats['total']}")
        print(f"✓ Uploaded:     {self.stats['uploaded']}")
        print(f"✗ Failed:       {self.stats['failed']}")
        print(f"⊘ Skipped:      {self.stats['skipped']}")
        print(f"{'='*60}")

        if self.errors:
            print("\n⚠️  Errors encountered:")
            for error in self.errors[:10]:  # Show first 10 errors
                print(f"  - {error['file']}: {error['error']}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")

        if self.dry_run:
            print("\n✓ Dry run completed successfully")
        elif self.stats["failed"] == 0:
            print("\n✓ All files uploaded successfully!")
        elif self.stats["uploaded"] > 0:
            print(f"\n⚠️  Upload completed with {self.stats['failed']} error(s)")
        else:
            print("\n❌ Upload failed - no files were uploaded")
            sys.exit(1)


def main():
    """Main entry point for the uploader script."""
    parser = argparse.ArgumentParser(
        description="Upload text files to Wren AI as knowledge instructions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all files from wren_knowledge directory (default)
  python services/wren_uploader.py

  # Dry run to preview what would be uploaded
  python services/wren_uploader.py --dry-run

  # Upload from custom directory
  python services/wren_uploader.py --input-dir my_knowledge

  # Upload to custom Wren AI instance
  python services/wren_uploader.py --base-url http://wren.example.com:3000

  # Create question-matching instructions instead of global
  python services/wren_uploader.py --question-matching
        """,
    )

    parser.add_argument(
        "--input-dir",
        "-i",
        type=str,
        default="wren_knowledge",
        help="Directory containing .txt files to upload (default: wren_knowledge)",
    )

    parser.add_argument(
        "--base-url",
        "-u",
        type=str,
        default="http://localhost:3001",
        help="Base URL of Wren AI instance (default: http://localhost:3001)",
    )

    parser.add_argument(
        "--api-key",
        "-k",
        type=str,
        help="Optional API key for authentication",
    )

    instruction_type = parser.add_mutually_exclusive_group()
    instruction_type.add_argument(
        "--global",
        dest="is_global",
        action="store_true",
        default=True,
        help="Create global instructions (default)",
    )
    instruction_type.add_argument(
        "--question-matching",
        dest="is_global",
        action="store_false",
        help="Create question-matching instructions",
    )

    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="Preview what would be uploaded without actually uploading",
    )

    args = parser.parse_args()

    # Create uploader and run
    uploader = WrenUploader(
        base_url=args.base_url,
        api_key=args.api_key,
        is_global=args.is_global,
        dry_run=args.dry_run,
    )

    input_dir = Path(args.input_dir)
    uploader.upload_directory(input_dir)


if __name__ == "__main__":
    main()
