#!/usr/bin/env python3

"""
create_test_dag.py
Create a test copy of a DAG file with modified dag_id

Usage: ./create_test_dag.py [OPTIONS] <dag_file_path>
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


# Colors for terminal output
class Colors:
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    CYAN = "\033[0;36m"
    NC = "\033[0m"  # No Color


def print_error(message):
    """Print error message in red"""
    print(f"{Colors.RED}❌ Error: {message}{Colors.NC}", file=sys.stderr)


def print_success(message):
    """Print success message in green"""
    print(f"{Colors.GREEN}✅ {message}{Colors.NC}")


def print_info(message):
    """Print info message in yellow"""
    print(f"{Colors.YELLOW}ℹ️  {message}{Colors.NC}")


def print_detail(label, value):
    """Print detail in cyan"""
    print(f"{Colors.CYAN}{label}:{Colors.NC} {value}")


def get_username():
    """Get username from git config or fallback to whoami"""
    try:
        # Try git config first
        result = subprocess.run(
            ["git", "config", "user.name"], capture_output=True, text=True, check=True
        )
        username = result.stdout.strip()
        if username:
            # Convert to lowercase and replace spaces with underscores
            return username.lower().replace(" ", "_")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    try:
        # Fallback to whoami
        result = subprocess.run(["whoami"], capture_output=True, text=True, check=True)
        return result.stdout.strip().lower()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Final fallback
    return "user"


def find_dag_id_patterns(content):
    """
    Find all DAG_NAME and dag_id patterns in the file
    Returns list of (line_number, pattern, original_value) tuples
    """
    patterns = []
    lines = content.split("\n")

    for i, line in enumerate(lines):
        # Pattern 1: DAG_NAME = "value"
        match = re.search(r'(DAG_NAME\s*=\s*["\'])([^"\']+)(["\'])', line)
        if match:
            patterns.append((i, "DAG_NAME", match.group(2), match))
            continue

        # Pattern 2: dag_id="value" or dag_id = "value"
        match = re.search(r'(dag_id\s*=\s*["\'])([^"\']+)(["\'])', line)
        if match:
            patterns.append((i, "dag_id", match.group(2), match))

    return patterns


def modify_content(content, suffix):
    """
    Modify the content by adding suffix to all dag_id patterns
    Returns (modified_content, list of changes)
    """
    lines = content.split("\n")
    changes = []

    for i, line in enumerate(lines):
        # Pattern 1: DAG_NAME = "value"
        match = re.search(r'(DAG_NAME\s*=\s*["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            new_line = line.replace(
                f"{match.group(1)}{original}{match.group(3)}",
                f"{match.group(1)}{new_value}{match.group(3)}",
            )
            lines[i] = new_line
            changes.append(("DAG_NAME", original, new_value))
            continue

        # Pattern 2: dag_id="value" or dag_id = "value"
        match = re.search(r'(dag_id\s*=\s*["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            new_line = line.replace(
                f"{match.group(1)}{original}{match.group(3)}",
                f"{match.group(1)}{new_value}{match.group(3)}",
            )
            lines[i] = new_line
            changes.append(("dag_id", original, new_value))

    return "\n".join(lines), changes


def generate_output_path(input_path, suffix):
    """
    Generate output path from input path and suffix
    Example: my_dag.py + _test_john -> my_dag_test_john.py
    """
    path = Path(input_path)
    stem = path.stem  # filename without extension
    extension = path.suffix  # .py

    # Remove leading underscore from suffix if present
    suffix = suffix.lstrip("_")

    new_name = f"{stem}_{suffix}{extension}"
    return path.parent / new_name


def validate_dag_file(file_path):
    """Validate the DAG file exists and is in correct location"""
    path = Path(file_path)

    # Check if file exists
    if not path.exists():
        print_error(f"File does not exist: {file_path}")
        return False

    # Check if it's a file
    if not path.is_file():
        print_error(f"Path is not a file: {file_path}")
        return False

    # Check if it's a Python file
    if path.suffix != ".py":
        print_error(f"File must be a Python file (.py): {file_path}")
        return False

    # Try to get git root and validate location
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
            cwd=path.parent,
        )
        git_root = Path(result.stdout.strip())

        # Get relative path from git root
        try:
            relative_path = path.resolve().relative_to(git_root)

            # Check if inside orchestration/dags/
            if not str(relative_path).startswith("orchestration/dags/"):
                print_error("File must be inside orchestration/dags/ directory")
                print_info(f"Current path: {relative_path}")
                print_info("Expected pattern: orchestration/dags/...")
                return False
        except ValueError:
            # File is not relative to git root
            print_error("File must be inside the git repository")
            return False

    except (subprocess.CalledProcessError, FileNotFoundError):
        print_info("Warning: Not in a git repository (validation skipped)")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create a test copy of a DAG file with modified dag_id",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-generate with username suffix
  %(prog)s orchestration/dags/jobs/admin/my_dag.py

  # Use custom suffix
  %(prog)s --suffix debug orchestration/dags/jobs/admin/my_dag.py

  # Specify custom output path
  %(prog)s --output orchestration/dags/test/my_test.py orchestration/dags/jobs/admin/my_dag.py

  # Dry-run to preview changes
  %(prog)s --dry-run orchestration/dags/jobs/admin/my_dag.py
        """,
    )

    parser.add_argument("dag_file", help="Path to the DAG file to copy")

    parser.add_argument(
        "--suffix",
        help="Custom suffix to add to dag_id (default: test_<username>)",
        default=None,
    )

    parser.add_argument(
        "--output",
        help="Custom output file path (default: auto-generated)",
        default=None,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without creating the file",
    )

    args = parser.parse_args()

    # Validate input file
    if not validate_dag_file(args.dag_file):
        sys.exit(1)

    input_path = Path(args.dag_file).resolve()

    # Read the file
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print_error(f"Failed to read file: {e}")
        sys.exit(1)

    # Determine suffix
    if args.suffix:
        suffix = f"_{args.suffix}" if not args.suffix.startswith("_") else args.suffix
    else:
        username = get_username()
        suffix = f"_test_{username}"

    # Find patterns in original content
    patterns = find_dag_id_patterns(content)

    if not patterns:
        print_error("No DAG_NAME or dag_id found in the file")
        print_info("This file might not be a valid DAG file")
        sys.exit(1)

    # Modify content
    modified_content, changes = modify_content(content, suffix)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = generate_output_path(input_path, suffix)

    # Preview mode
    if args.dry_run:
        print()
        print_info("DRY-RUN MODE: No files will be created")
        print()
        print_detail("Input ", str(input_path))
        print_detail("Output", str(output_path))
        print()
        print("Changes to be made:")
        for pattern_type, original, new_value in changes:
            print(f'  {pattern_type}: "{original}" → "{new_value}"')
        print()
        print_success("Dry-run complete. Run without --dry-run to create the file.")
        sys.exit(0)

    # Check if output file already exists
    if output_path.exists():
        print_error(f"Output file already exists: {output_path}")
        print_info("Please delete it first or use --output to specify a different path")
        sys.exit(1)

    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the modified content
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(modified_content)
    except Exception as e:
        print_error(f"Failed to write file: {e}")
        sys.exit(1)

    # Success!
    print()
    print_success("Test DAG created successfully!")
    print_detail("Input ", str(input_path))
    print_detail("Output", str(output_path))
    print()
    if changes:
        for pattern_type, original, new_value in changes:
            print(f'{pattern_type} changed: "{original}" → "{new_value}"')
    print()
    print_info("Next steps:")
    print(f"  1. Review the file: {output_path}")
    print(
        f"  2. Upload to staging: ./orchestration/scripts/push_to_staging.sh {output_path}"
    )
    print()


if __name__ == "__main__":
    main()
