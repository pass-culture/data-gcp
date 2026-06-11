#!/usr/bin/env python3

"""
create_test_dag.py — Create a test copy of a DAG file with a suffixed dag_id.

WHY THIS SCRIPT EXISTS:
-----------------------
Each Airflow DAG must have a unique dag_id. When testing changes you need a copy
that coexists with the production DAG without conflict. Creating that copy manually
is error-prone: you have to find every dag_id reference, pick a unique suffix, keep
the filename consistent, and remember to disable the schedule so it never triggers
automatically. This script handles all of that in one step.

WHAT THIS SCRIPT DOES:
----------------------
1. Renames the dag_id by adding a suffix (default: __test__<your-username>) to:
     - DAG_NAME / DAG_ID module-level string constants
     - dag_id='...' literal keyword argument inside DAG() / @dag()
2. Sets schedule= / schedule_interval= to None inside the DAG() constructor so
   the test copy only runs when triggered manually.
3. Writes the modified file next to the original, with the suffix in the filename.
4. Fails loudly (exit 1) if the dag_id cannot be renamed or the schedule cannot be
   found, rather than silently producing a broken or conflicting copy.

Supported dag_id patterns (anything else causes an explicit error):
  DAG_NAME = "..."       module-level constant (most common)
  DAG_ID   = "..."       alternative constant name
  dag_id   = "..."       literal keyword arg inside DAG() / @dag()

USE CASES:
----------
- Test a DAG change on dev/stg without touching the production version.
- Run a one-off backfill under a throwaway dag_id.
- Share a work-in-progress DAG with a teammate without schedule conflicts.

Usage: ./create_test_dag.py [OPTIONS] <dag_file_path>
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_COPY_BASE_SUFFIX = "__test__"


class Colors:
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    CYAN = "\033[0;36m"
    NC = "\033[0m"


def print_error(message: str) -> None:
    print(f"{Colors.RED}❌ Error: {message}{Colors.NC}", file=sys.stderr)


def print_success(message: str) -> None:
    print(f"{Colors.GREEN}✅ {message}{Colors.NC}")


def print_info(message: str) -> None:
    print(f"{Colors.YELLOW}ℹ️  {message}{Colors.NC}")


def print_detail(label: str, value: str) -> None:
    print(f"{Colors.CYAN}{label}:{Colors.NC} {value}")


# ---------------------------------------------------------------------------
# Username
# ---------------------------------------------------------------------------


def get_username() -> str:
    try:
        result = subprocess.run(["whoami"], capture_output=True, text=True, check=True)
        return result.stdout.strip().lower()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    return "airflow_user"


# ---------------------------------------------------------------------------
# DAG constructor locator
# ---------------------------------------------------------------------------


def find_dag_constructor_range(lines: list[str]) -> tuple[int, int]:
    """
    Locate the DAG() call lines positions

    Handles:  with DAG(...)  |  dag = DAG(...)  |  with GridDAG(...)
    Anchored to line start so strings like logging.info("test DAG(s)") never match.
    Returns (start_line, end_line) inclusive, or raises ValueError.
    """
    # ^\s* — anchored to avoid matching DAG( inside strings or nested calls
    # \w*DAG — covers subclasses like GridDAG
    # (?:with\s+)? / (?:[\w.]+\s*=\s*)? — context-manager and assignment prefixes
    dag_line_re = re.compile(r"^\s*(?:(?:with\s+)?(?:[\w.]+\s*=\s*)?\w*DAG)\s*\(")
    decorator_fail_re = re.compile(r"^\s*@dag\b")

    # Parsing Python using raw string indices instead of the 'ast' module is a sin and it's exactly what this function does...
    # we start by digging through raw strings purgatory
    for i, line in enumerate(lines):
        if re.match(r"\s*#", line):
            continue

        if decorator_fail_re.match(line):
            raise ValueError(
                f"Line {i+1}: Found an unsupported '@dag' decorator. How elegant of you! "
                "Alas, these desolated lands are impervious to TaskFlow spells. Either use a classic DAG constructor, "
                "or become the architect of your own damnation and build a parser that can perfuse your fancy magic"
            )

        m = dag_line_re.match(line)
        if m:
            # You have digged enough to reach Hell!
            #  As a welcome pack and to pay for your sins you have been sentenced to ... counting parenthesis
            paren_pos = m.end() - 1  # position of the opening '('
            depth = 0
            for j in range(i, len(lines)):
                # first line we remove everything before the first '('  subsequent lines we keep all
                seg = lines[j] if j > i else lines[j][paren_pos:]
                for ch in seg:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if (
                            depth == 0
                        ):  # we are finally leaving this endless loop and going home
                            return (i, j)
            # unless the file you are trying to modify is not valid anyway
            if depth != 0:
                raise SyntaxError(
                    "DAG(...) constructor has unbalanced parenthesis - beware comments count as well"
                )
    # or you are lost and are not using this to deploy a dag file
    raise ValueError("No DAG(...) constructor or @dag decorator found in file")


# ---------------------------------------------------------------------------
# Content rewrite helpers (each handles one pattern)
# ---------------------------------------------------------------------------


def _splice(line: str, match: re.Match[str], new_inner: str) -> str:
    """
    Space insensitive replace match group 2 with new_inner, preserving group 1, group 3, and the rest of the line.

    Regexps in this script use three capture groups around the value to replace:
      group 1 — prefix including opening quote, e.g. 'DAG_NAME = "'
      group 2 — the value itself,              e.g. 'my_dag'
      group 3 — closing quote,                 e.g. '"'

    Example:

      line  = 'DAG_NAME = "my_dag"  # some comment'
      match on r'(DAG_NAME\\s*=\\s*f?["\'])([^"\']+)(["\'])'
      _splice(line, match, "my_dag__test__john")
      → 'DAG_NAME = "my_dag__test__john"  # some comment'

    Using character positions (match.start/end) rather than str.replace ensures only
    this exact occurrence is modified and neighbouring text is never touched.
    """
    return (
        line[: match.start(1)]
        + match.group(1)
        + new_inner
        + match.group(3)
        + line[match.end(3) :]
    )


def _rename_dag_constant(lines: list[str], suffix: str) -> tuple[str, str, str] | None:
    """
    Rename DAG_NAME or DAG_ID string constant at module level.

    Most DAGs pass the constant as a variable (DAG(DAG_NAME, ...) or dag_id=DAG_NAME),
    so renaming the constant is the only way to change the dag_id in those cases.
    Position-based splice avoids corrupting other params that share the same string value.
    Returns (const_name, old, new) or None.
    """
    for i, line in enumerate(lines):
        # Skip # line comment
        if re.match(r"\s*#", line):
            continue
        # Group 1: prefix up to opening quote  Group 2: value  Group 3: closing quote
        # f? handles f-strings: suffix appended inside the template stays valid Python
        match = re.search(r'(DAG_(?:NAME|ID)\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            # Mutate the shared list in place so later passes (constructor range lookup,
            # dag_id kwarg rename) see the already-updated content.
            lines[i] = _splice(line, match, new_value)
            const_name = match.group(1).split("=")[0].strip()  # "DAG_NAME" or "DAG_ID"
            return (const_name, original, new_value)
    return None


def _rename_dag_id_kwarg(
    lines: list[str], dag_start: int, dag_end: int, suffix: str
) -> tuple[str, str, str] | None:
    """
    Rename dag_id='...' literal keyword argument inside the DAG() constructor.

    Only fires when the id is a literal string, not a variable reference.
    ``(?<!\\w)`` prevents matching external_dag_id= or other params ending in dag_id.
    f? allows f-strings: dag_id=f"my_dag_{ENV}" → dag_id=f"my_dag_{ENV}__test__john".
    Returns (const_name, old, new) or None.
    """
    for i in range(dag_start, dag_end + 1):
        line = lines[i]
        # skip comment line
        if re.match(r"\s*#", line):
            continue

        # match a dag_id="litteral_or_f-string" and suffix it
        match = re.search(r'(?<!\w)(dag_id\s*=\s*f?["\'])([^"\']+)(["\'])', line)
        if match:
            original = match.group(2)
            new_value = f"{original}{suffix}"
            lines[i] = _splice(line, match, new_value)
            return ("dag_id", original, new_value)
    return None


def _end_of_value(lines: list[str], start_idx: int, first_fragment: str) -> int:
    """
    Return the exclusive end index after consuming continuation lines until
    the open parentheses in first_fragment balance. Used for multi-line values
    like get_airflow_schedule(\\n    ...\\n).
    """
    depth = first_fragment.count("(") - first_fragment.count(")")
    j = start_idx + 1
    while depth > 0 and j < len(lines):
        depth += lines[j].count("(") - lines[j].count(")")
        j += 1
    return j


def _replace_schedule(
    lines: list[str], dag_start: int, dag_end: int
) -> tuple[list[str], tuple[str, str, str] | None, bool]:
    """
    Replace schedule= / schedule_interval= with None inside the DAG() constructor.

    schedule(?:_interval)? covers Airflow 2.4+ 'schedule=' and legacy 'schedule_interval='.
    ``(?<!\\w)`` prevents false matches on dag_schedule= or similar variable names.
    Multi-line values (e.g. get_airflow_schedule(\\n...\\n)) are consumed via paren-depth
    balancing so no dangling syntax is left behind.

    Returns (new_lines, change_tuple | None, found: bool).
      found=False  → schedule param absent from constructor (caller should abort).
      change=None  → param was already None, no rewrite needed.
    """
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Outside of DAG constructor - copy line as is
        in_constructor = dag_start <= i <= dag_end
        if not in_constructor:
            new_lines.append(line)
            i += 1
            continue

        # In DAG constructor copy other parameters as is
        match = re.search(r"(?<!\w)(schedule(?:_interval)?\s*=\s*)(.+)", line)
        if not (match and not re.match(r"\s*#", line)):
            new_lines.append(line)
            i += 1
            continue

        # Replace schedule by None, when the line contains schedule or schedule_interval parametrer
        # 1 - Strip inline comment and trailing whitespace from the RHS
        rest = match.group(2)
        hash_pos = rest.find("#")
        comment = ("  " + rest[hash_pos:].rstrip()) if hash_pos >= 0 else ""
        rest = rest[:hash_pos].rstrip() if hash_pos >= 0 else rest.rstrip()
        current_val = rest.rstrip(",").strip()
        param_name = match.group(1).rstrip().rstrip("=").rstrip()

        # If already None, copy line as is
        if current_val == "None":
            new_lines.extend(lines[i:])
            return new_lines, None, True

        # Consume multi-line continuation, then replace the whole value with None
        end_idx = _end_of_value(lines, i, rest)
        # Preserve the trailing comma so the surrounding argument list stays valid
        last_line = lines[end_idx - 1].rstrip() if end_idx > i + 1 else rest
        trailing = "," if last_line.endswith(",") else ""

        new_lines.append(
            line[: match.start(1)] + match.group(1) + "None" + trailing + comment
        )
        new_lines.extend(lines[end_idx:])
        return new_lines, (param_name, current_val, "None"), True

    return new_lines, None, False  # schedule param not found in constructor


# ---------------------------------------------------------------------------
# Main rewrite orchestrator
# ---------------------------------------------------------------------------


def modify_content(content: str, suffix: str) -> tuple[str, list[tuple[str, str, str]]]:
    """
    Apply all dag_id and schedule rewrites. Returns (modified_content, changes).
    Exits with a clear error if either rewrite cannot be completed safely.

    Limitations/quirks:

    Assuming a dev named dumb_dev is writing/refactoring a dag and uses this script to test it's deployment.
    If the dag .py file contains both:
        a variable DAG_ID="my_dag"
        and DAG(dag_id=f"{DAG_ID}",...)

    the copied dag will be named "my_dag__test__dumb_dev__test__dumb_dev"
    and to be honest he or she deserves it because regex are all fun & games until someone loses an eye
    """
    lines = content.split("\n")
    changes = []

    # 1. Rename DAG_NAME / DAG_ID module-level constant
    change = _rename_dag_constant(lines, suffix)
    if change:
        changes.append(change)

    # 2. Locate the DAG() constructor block
    try:
        dag_start, dag_end = find_dag_constructor_range(lines)
    except ValueError as e:
        print_error(str(e))
        sys.exit(1)

    # 3. Rename dag_id='...' literal keyword arg inside the constructor
    change = _rename_dag_id_kwarg(lines, dag_start, dag_end, suffix)
    if change:
        changes.append(change)

    # 4. Replace schedule= / schedule_interval= with None inside the constructor
    lines, change, schedule_found = _replace_schedule(lines, dag_start, dag_end)
    if not schedule_found:
        print_error(
            "Neither 'schedule' nor 'schedule_interval' found inside the DAG(...) "
            "constructor — cannot guarantee the test DAG won't run automatically. Aborting."
        )
        sys.exit(1)
    if change:
        changes.append(change)

    # 5. Guard: ensure the dag_id was actually renamed (unsupported patterns like DagConfig)
    if not any(t in ("DAG_NAME", "DAG_ID", "dag_id") for t, *_ in changes):
        print_error(
            "Could not rename the dag_id — the test copy would conflict with the "
            "production DAG in Airflow. Aborting.\n"
            "The script renames: DAG_NAME = '...', DAG_ID = '...', or dag_id='...' "
            "inside the DAG() constructor.\n"
            "This DAG uses none of those patterns. Rename the id manually after creation "
            "with --force, or refactor the DAG to use one of the supported patterns."
        )
        sys.exit(1)

    return "\n".join(lines), changes


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def generate_output_path(input_path: Path, suffix: str) -> Path:
    """my_dag.py + __test__john  →  my_dag__test__john.py  (same directory)."""
    path = Path(input_path)
    new_name = f"{path.stem}{suffix}{path.suffix}"
    return path.parent / new_name


def validate_dag_file(file_path: str) -> bool:
    """Check the file exists, is a file, and is a .py file."""
    path = Path(file_path)

    if not path.exists():
        print_error(f"File does not exist: {file_path}")
        return False
    if not path.is_file():
        print_error(f"Path is not a file: {file_path}")
        return False
    if path.suffix != ".py":
        print_error(f"File must be a Python file (.py): {file_path}")
        return False

    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
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
        "--suffix", help="Custom suffix (default: test_<username>)", default=None
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
    parser.add_argument("--no-hint", action="store_true", help="Hide next steps hints")
    args = parser.parse_args()

    if not validate_dag_file(args.dag_file):
        sys.exit(1)

    input_path = Path(args.dag_file).resolve()

    try:
        content = input_path.read_text(encoding="utf-8")
    except Exception as e:
        print_error(f"Failed to read file: {e}")
        sys.exit(1)

    suffix = (
        (
            f"_{args.suffix}"
            if args.suffix and not args.suffix.startswith("_")
            else args.suffix
        )
        if args.suffix
        else f"{DEFAULT_COPY_BASE_SUFFIX}{get_username()}"
    )

    modified_content, changes = modify_content(content, suffix)

    output_path = (
        Path(args.output) if args.output else generate_output_path(input_path, suffix)
    )

    if args.dry_run:
        print()
        print_info("DRY-RUN MODE: No files will be created")
        print()
        print_detail("Input ", str(input_path))
        print_detail("Output", str(output_path))
        print()
        print("Changes to be made:")
        for kind, original, new_value in changes:
            print(f'  {kind}: "{original}" → "{new_value}"')
        print()
        print_success("Dry-run complete. Run without --dry-run to create the file.")
        sys.exit(0)

    if output_path.exists() and not args.force:
        print_error(f"Output file already exists: {output_path}")
        print_info("Use --force to overwrite or --output to specify a different path")
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        output_path.write_text(modified_content, encoding="utf-8")
    except Exception as e:
        print_error(f"Failed to write file: {e}")
        sys.exit(1)

    filename = output_path.name
    print()
    print_success("Test DAG created successfully!")
    print_detail("Input ", str(input_path))
    print_detail("Output", str(output_path))
    print()
    for kind, original, new_value in changes:
        print(f'{kind} changed: "{original}" → "{new_value}"')
    print()
    if not args.no_hint:
        print_info("Next steps:")
        print(f"  1. Review the file: {output_path}")
        print("  2. Push to an environment (from orchestration/):")
        print(f"       dev  : make push-dag DAG={filename}")
        print(f"       stg  : make push-dag DAG={filename} ENV=stg")
        print(f"       prod : make push-dag DAG={filename} ENV=prod")
        print()


if __name__ == "__main__":
    main()
