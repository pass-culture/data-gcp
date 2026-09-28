import importlib
import sys
from pathlib import Path

import typer

app = typer.Typer()

CLI_DIR = Path(__file__).parent / "cli"
DOMAINS = sorted(p.stem for p in CLI_DIR.glob("*.py") if p.stem != "__init__")


def main() -> None:
    """Dispatch to a single domain's Typer app: <prog> <domain> <command> ...

    Only the requested domain's cli module is imported and registered, so a
    caller that installed just one optional-dependency group (see
    pyproject.toml) doesn't need the other domains' heavier dependencies just
    to invoke this entrypoint.
    """
    if len(sys.argv) < 2 or sys.argv[1] not in DOMAINS:
        prog = Path(sys.argv[0]).name
        sys.stderr.write(f"Usage: {prog} {{{'|'.join(DOMAINS)}}} <command> [args]...\n")
        sys.exit(1)

    domain = sys.argv[1]
    domain_app = importlib.import_module(f"cli.{domain}").app
    app.add_typer(domain_app, name=domain)
    app()


if __name__ == "__main__":
    main()
