#!/usr/bin/env python3
"""
Delete all knowledge instructions from Wren AI.

This script retrieves all instructions from a Wren AI instance
and deletes them via the Knowledge Instructions API.
"""

import argparse
import sys
from typing import Dict, List, Optional

import requests


class WrenCleanup:
    """Delete all instructions from Wren AI."""

    def __init__(
        self,
        base_url: str = "http://localhost:3001",
        api_key: Optional[str] = None,
        dry_run: bool = False,
    ):
        """
        Initialize the Wren cleanup tool.

        Args:
            base_url: Base URL of Wren AI instance
            api_key: Optional API key for authentication
            dry_run: If True, list instructions without deleting
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.dry_run = dry_run
        self.api_endpoint = f"{self.base_url}/api/v1/knowledge/instructions"

        # Statistics
        self.stats = {
            "total": 0,
            "deleted": 0,
            "failed": 0,
        }
        self.errors: List[Dict[str, str]] = []

    def get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def get_all_instructions(self) -> List[Dict]:
        """
        Retrieve all instructions from Wren AI.

        Returns:
            List of instruction objects
        """
        try:
            response = requests.get(
                self.api_endpoint,
                headers=self.get_headers(),
                timeout=30,
            )

            if response.status_code == 200:
                instructions = response.json()
                return instructions
            else:
                print(
                    f"❌ Failed to retrieve instructions: HTTP {response.status_code}"
                )
                try:
                    error_data = response.json()
                    print(f"   Error: {error_data.get('error', 'Unknown error')}")
                except Exception:
                    pass
                return []

        except requests.exceptions.ConnectionError:
            print(f"❌ Cannot connect to Wren AI at {self.base_url}")
            print("   Make sure Wren AI is running")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error retrieving instructions: {e}")
            sys.exit(1)

    def delete_instruction(self, instruction_id: int) -> bool:
        """
        Delete a single instruction.

        Args:
            instruction_id: ID of the instruction to delete

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            return True

        try:
            response = requests.delete(
                f"{self.api_endpoint}/{instruction_id}",
                headers=self.get_headers(),
                timeout=30,
            )

            if response.status_code in [200, 204]:
                return True
            else:
                error_msg = f"HTTP {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", error_msg)
                except Exception:
                    pass

                print(f"  ✗ Failed to delete ID {instruction_id}: {error_msg}")
                self.errors.append({"id": str(instruction_id), "error": error_msg})
                return False

        except Exception as e:
            print(f"  ✗ Failed to delete ID {instruction_id}: {e}")
            self.errors.append({"id": str(instruction_id), "error": str(e)})
            return False

    def list_instructions(self, instructions: List[Dict]) -> None:
        """
        Display all instructions in a readable format.

        Args:
            instructions: List of instruction objects
        """
        print(f"\n{'='*80}")
        print("Instructions in Wren AI")
        print(f"{'='*80}\n")

        if not instructions:
            print("No instructions found.")
            return

        for i, instr in enumerate(instructions, 1):
            instr_id = instr.get("id", "unknown")
            instr_text = instr.get("instruction", "")
            is_global = instr.get("isGlobal", False)
            created_at = instr.get("createdAt", "")

            # Truncate long instructions
            display_text = (
                instr_text[:100] + "..." if len(instr_text) > 100 else instr_text
            )

            print(
                f"[{i}] ID: {instr_id} | Type: {'Global' if is_global else 'Question-matching'}"
            )
            print(f"    Created: {created_at}")
            print(f"    Content: {display_text}")
            print()

    def cleanup(self, auto_confirm: bool = False) -> None:
        """
        Main cleanup process: retrieve and delete all instructions.

        Args:
            auto_confirm: If True, skip confirmation prompt
        """
        print(f"\n{'='*80}")
        print("Wren AI Knowledge Cleanup")
        print(f"{'='*80}")
        print(f"Wren AI URL: {self.base_url}")
        if self.dry_run:
            print("Mode: DRY RUN (no deletions)")
        print(f"{'='*80}\n")

        # Retrieve all instructions
        print("🔍 Retrieving instructions...")
        instructions = self.get_all_instructions()

        self.stats["total"] = len(instructions)

        if not instructions:
            print("✓ No instructions found. Nothing to delete.")
            return

        # Display instructions
        self.list_instructions(instructions)

        print(f"{'='*80}")
        print(f"Found {self.stats['total']} instruction(s)")
        print(f"{'='*80}\n")

        # Confirmation prompt
        if not self.dry_run and not auto_confirm:
            response = (
                input("⚠️  Delete all these instructions? (yes/no): ").strip().lower()
            )
            if response not in ["yes", "y"]:
                print("❌ Deletion cancelled.")
                sys.exit(0)

        if self.dry_run:
            print("✓ Dry run completed. No instructions were deleted.")
            return

        # Delete instructions
        print("\n🗑️  Deleting instructions...\n")

        for i, instr in enumerate(instructions, 1):
            instr_id = instr.get("id")
            if instr_id is None:
                print("  ⊘ Skipped: Invalid instruction (no ID)")
                continue

            print(f"[{i}/{self.stats['total']}] Deleting instruction ID {instr_id}...")

            success = self.delete_instruction(instr_id)

            if success:
                self.stats["deleted"] += 1
                print("  ✓ Deleted")
            else:
                self.stats["failed"] += 1

        # Print summary
        self.print_summary()

    def print_summary(self) -> None:
        """Print deletion summary statistics."""
        print(f"\n{'='*80}")
        print("Cleanup Summary")
        print(f"{'='*80}")
        print(f"Total instructions: {self.stats['total']}")
        print(f"✓ Deleted:          {self.stats['deleted']}")
        print(f"✗ Failed:           {self.stats['failed']}")
        print(f"{'='*80}")

        if self.errors:
            print("\n⚠️  Errors encountered:")
            for error in self.errors[:10]:  # Show first 10 errors
                print(f"  - ID {error['id']}: {error['error']}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")

        if self.stats["failed"] == 0 and self.stats["deleted"] > 0:
            print("\n✓ All instructions deleted successfully!")
        elif self.stats["deleted"] == 0 and self.stats["failed"] > 0:
            print("\n❌ Cleanup failed - no instructions were deleted")
            sys.exit(1)
        elif self.stats["failed"] > 0:
            print(f"\n⚠️  Cleanup completed with {self.stats['failed']} error(s)")


def main():
    """Main entry point for the cleanup script."""
    parser = argparse.ArgumentParser(
        description="Delete all knowledge instructions from Wren AI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all instructions without deleting (dry-run)
  python services/wren_cleanup.py --dry-run

  # Delete all instructions with confirmation
  python services/wren_cleanup.py

  # Delete all without confirmation
  python services/wren_cleanup.py --yes

  # Connect to custom Wren AI instance
  python services/wren_cleanup.py --base-url http://wren.example.com:3001
        """,
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

    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="List instructions without deleting them",
    )

    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and delete immediately",
    )

    args = parser.parse_args()

    # Create cleanup tool and run
    cleanup = WrenCleanup(
        base_url=args.base_url,
        api_key=args.api_key,
        dry_run=args.dry_run,
    )

    cleanup.cleanup(auto_confirm=args.yes)


if __name__ == "__main__":
    main()
