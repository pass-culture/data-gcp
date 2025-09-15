import subprocess
import tempfile
from pathlib import Path

import typer

BASE_PATH = Path(__file__).resolve().parent.parent

UV_EXPORT_CMD = [
    "uv",
    "export",
    "--no-hashes",
    "--no-annotate",
    "--no-header",
    "--format",
    "requirements-txt",
    "-o",
]


def get_uv_lock_dir() -> Path:
    """Get the directory containing the `uv.lock` file."""
    return [p.parent for p in BASE_PATH.glob("**/uv.lock")]


app = typer.Typer()


def export_requirements_for_branch(branch: str, output_dir: Path) -> None:
    """Export requirements for a specific git branch."""
    # Stash current changes if any
    subprocess.run(["git", "stash"], cwd=BASE_PATH, capture_output=True)

    # Checkout the branch
    result = subprocess.run(
        ["git", "checkout", branch], cwd=BASE_PATH, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise typer.Exit(f"Failed to checkout branch '{branch}': {result.stderr}")

    # Export requirements for all uv.lock directories
    uv_lock_dirs = get_uv_lock_dir()
    for uv_lock_dir in uv_lock_dirs:
        relative_path = uv_lock_dir.relative_to(BASE_PATH)
        output_file = (
            output_dir / f"{str(relative_path).replace('/', '_')}_requirements.txt"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        print(f"Exporting requirements from {uv_lock_dir} (branch: {branch})")
        subprocess.run(
            UV_EXPORT_CMD + [str(output_file)],
            cwd=uv_lock_dir,
            check=True,
        )


@app.command()
def export_requirements(
    prefix: str = typer.Option(..., help="The prefix to use for the output file."),
) -> None:
    """Compile the requirements for all jobs."""
    import subprocess

    uv_lock_dirs = get_uv_lock_dir()
    output_filename = f"{prefix}requirements.txt"
    for uv_lock_dir in uv_lock_dirs:
        print(f"Compiling requirements in {uv_lock_dir}")
        subprocess.run(
            UV_EXPORT_CMD + [output_filename],
            cwd=uv_lock_dir,
            check=True,
        )


@app.command()
def diff_requirements(
    branch1: str = typer.Option(..., help="First branch to compare"),
    branch2: str = typer.Option(..., help="Second branch to compare"),
    output_file: str = typer.Option(None, help="Output file for the diff (optional)"),
) -> None:
    """Export requirements from two branches and show the diff."""

    # Store current branch
    current_branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=BASE_PATH,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            branch1_dir = temp_path / "branch1"
            branch2_dir = temp_path / "branch2"

            # Export requirements for both branches
            export_requirements_for_branch(branch1, branch1_dir)
            export_requirements_for_branch(branch2, branch2_dir)

            # Generate diff for each requirements file
            diff_output = []
            branch1_files = list(branch1_dir.glob("*_requirements.txt"))

            for branch1_file in branch1_files:
                branch2_file = branch2_dir / branch1_file.name

                if branch2_file.exists():
                    # Run diff command
                    diff_result = subprocess.run(
                        ["diff", "-u", str(branch1_file), str(branch2_file)],
                        capture_output=True,
                        text=True,
                    )

                    if diff_result.returncode != 0:  # Files are different
                        diff_output.append(f"\n=== Diff for {branch1_file.name} ===")
                        diff_output.append(f"--- {branch1}/{branch1_file.name}")
                        diff_output.append(f"+++ {branch2}/{branch2_file.name}")
                        diff_output.append(diff_result.stdout)
                    else:
                        diff_output.append(
                            f"\n=== {branch1_file.name}: No differences ==="
                        )
                else:
                    diff_output.append(
                        f"\n=== {branch1_file.name}: Only exists in {branch1} ==="
                    )

            # Check for files only in branch2
            branch2_files = list(branch2_dir.glob("*_requirements.txt"))
            for branch2_file in branch2_files:
                branch1_file = branch1_dir / branch2_file.name
                if not branch1_file.exists():
                    diff_output.append(
                        f"\n=== {branch2_file.name}: Only exists in {branch2} ==="
                    )

            # Output results
            diff_text = "\n".join(diff_output)

            if output_file:
                with open(output_file, "w") as f:
                    f.write(diff_text)
                print(f"Diff saved to {output_file}")
            else:
                print(diff_text)

    finally:
        # Return to original branch
        subprocess.run(["git", "checkout", current_branch], cwd=BASE_PATH, check=True)
        # Restore stashed changes if any
        subprocess.run(["git", "stash", "pop"], cwd=BASE_PATH, capture_output=True)


if __name__ == "__main__":
    app()
