#!/usr/bin/env python3

"""
create_test_dag.py
Create a test copy of a DAG file with modified dag_id

WHY THIS SCRIPT EXISTS:
-----------------------
In Apache Airflow (and similar orchestration systems), each DAG must have a unique dag_id.
When developing or debugging DAGs, you often need to test changes without affecting the
production DAG. This requires creating a test copy with a different dag_id.

Manually copying and renaming DAGs is error-prone because:
1. You need to find and modify all dag_id references (DAG_NAME variables, dag_id parameters)
2. You need to ensure the new dag_id is unique (typically by adding your username)
3. You need to follow naming conventions for the output file
4. You need to ensure the file is in the correct directory structure

WHAT THIS SCRIPT DOES:
----------------------
This script automates the process of creating test DAG copies by:
1. Reading an existing DAG file from orchestration/dags/
2. Finding all dag_id patterns (DAG_NAME constants and dag_id parameters)
3. Adding a suffix to each dag_id (default: _test_<username>)
4. Generating an output filename with the same suffix
5. Creating the new file in the appropriate location
6. Validating that files are in the correct directory structure

USE CASES:
----------
- Testing DAG changes in isolation without affecting production
- Creating personal development versions of shared DAGs
- Debugging DAG issues in a safe environment
- Experimenting with DAG modifications before committing

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
        match = re.search(r'(DAG_NAME\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            patterns.append((i, "DAG_NAME", match.group(2), match))
            continue

        # Pattern 2: dag_id="value" or dag_id = "value"
        match = re.search(r'(?<!\w)(dag_id\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            patterns.append((i, "dag_id", match.group(2), match))

    return patterns


def modify_content(content, suffix):
    """
    Modify the content by adding suffix to all dag_id patterns and setting
    schedule_interval to None.
    Returns (modified_content, list of changes)
    """
    lines = content.split("\n")
    changes = []

    # Pass 1: DAG_NAME and dag_id (always single-line)
    for i, line in enumerate(lines):
        match = re.search(r'(DAG_NAME\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            lines[i] = line.replace(
                f"{match.group(1)}{original}{match.group(3)}",
                f"{match.group(1)}{new_value}{match.group(3)}",
            )
            changes.append(("DAG_NAME", original, new_value))
            continue

        match = re.search(r'(?<!\w)(dag_id\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            lines[i] = line.replace(
                f"{match.group(1)}{original}{match.group(3)}",
                f"{match.group(1)}{new_value}{match.group(3)}",
            )
            changes.append(("dag_id", original, new_value))

    # Pass 2: schedule_interval — may span multiple lines when the value
    # contains an open parenthesis (e.g. get_airflow_schedule(\n...\n))
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        match = re.search(r"(schedule_interval\s*=\s*)(.+)", line)
        if match:
            rest = match.group(2)
            hash_pos = rest.find("#")
            comment = ("  " + rest[hash_pos:].rstrip()) if hash_pos >= 0 else ""
            rest = rest[:hash_pos].rstrip() if hash_pos >= 0 else rest.rstrip()
            current_val = rest.rstrip(",").strip()

            if current_val != "None":
                # Consume continuation lines until parentheses balance
                paren_depth = rest.count("(") - rest.count(")")
                j = i + 1
                while paren_depth > 0 and j < len(lines):
                    paren_depth += lines[j].count("(") - lines[j].count(")")
                    j += 1

                last = lines[j - 1].rstrip() if j > i + 1 else rest
                trailing = "," if last.endswith(",") else ""
                new_lines.append(
                    line[: match.start()] + match.group(1) + "None" + trailing + comment
                )
                changes.append(("schedule_interval", current_val, "None"))
                i = j
                continue

        new_lines.append(line)
        i += 1

    return "\n".join(new_lines), changes


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

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists",
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
    if output_path.exists() and not args.force:
        print_error(f"Output file already exists: {output_path}")
        print_info("Use --force to overwrite or --output to specify a different path")
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
    filename = output_path.name
    print_info("Next steps:")
    print(f"  1. Review the file: {output_path}")
    print("  2. Push to an environment (from orchestration/):")
    print(f"       stg  : make push-dag DAG={filename}")
    print(f"       dev  : make push-dag DAG={filename} ENV=dev")
    print(f"       prod : make push-dag DAG={filename} ENV=prod")
    print()


if __name__ == "__main__":
    main()
