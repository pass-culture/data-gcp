import logging

import typer


class CLIState:
    """Global CLI state to track --verbose flag."""

    verbose: bool = False


state = CLIState()


class LogPrinter:
    """Logger + Typer print helper."""

    def __init__(self, logger: logging.Logger, state: CLIState):
        self.logger = logger
        self.state = state  # access global verbose flag

    def set_verbose(self, verbose: bool):
        """Update verbose state and logger level."""
        self.state.verbose = verbose
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    def debug(self, msg: str, **style):
        self.logger.debug(msg)
        if self.state.verbose:
            typer.secho(msg, **style)

    def info(self, msg: str, **style):
        self.logger.info(msg)
        if self.state.verbose:  # Only print if verbose
            fg = style.pop("fg", "cyan")
            typer.secho(msg, fg=fg, **style)

    def warning(self, msg: str, **style):
        self.logger.warning(msg)
        fg = style.pop("fg", "yellow")
        typer.secho(msg, fg=fg, **style)

    def error(self, msg: str, **style):
        self.logger.error(msg)
        fg = style.pop("fg", "red")
        typer.secho(msg, fg=fg, **style)

    def critical(self, msg: str, **style):
        self.logger.critical(msg)
        fg = style.pop("fg", "red")
        bold = style.pop("bold", True)
        typer.secho(msg, fg=fg, bold=bold, **style)


# --- Setup global logger ---
logger = logging.getLogger("external_reporting")
logger.setLevel(logging.DEBUG)  # default; LogPrinter filters output
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# Global instance to use everywhere
log_print = LogPrinter(logger, state)
