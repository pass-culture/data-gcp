import logging

import typer


class CLIState:
    """Global CLI state to track --verbose flag."""

    def __init__(self):
        self.verbose: bool = False

    def set_verbose(self, verbose: bool):
        self.verbose = verbose
        # Update the global logger level
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)


state = CLIState()


class LogPrinter:
    """Logger + Typer print helper."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def set_verbose(self, verbose: bool):
        """Proxy to global state."""
        state.set_verbose(verbose)

    def debug(self, msg: str, **style):
        self.logger.debug(msg)
        if self.logger.isEnabledFor(logging.DEBUG):
            typer.secho(msg, **style)

    def info(self, msg: str, **style):
        self.logger.info(msg)
        if self.logger.isEnabledFor(logging.INFO):
            fg = style.pop("fg", "cyan")
            typer.secho(msg, fg=fg, **style)

    def warning(self, msg: str, **style):
        self.logger.warning(msg)
        if self.logger.isEnabledFor(logging.WARNING):
            fg = style.pop("fg", "yellow")
            typer.secho(msg, fg=fg, **style)

    def error(self, msg: str, **style):
        self.logger.error(msg)
        if self.logger.isEnabledFor(logging.ERROR):
            fg = style.pop("fg", "red")
            typer.secho(msg, fg=fg, **style)

    def critical(self, msg: str, **style):
        self.logger.critical(msg)
        if self.logger.isEnabledFor(logging.CRITICAL):
            fg = style.pop("fg", "red")
            bold = style.pop("bold", True)
            typer.secho(msg, fg=fg, bold=bold, **style)


# --- Setup global logger ---
logger = logging.getLogger("external_reporting")
logger.setLevel(logging.WARNING)  # Default level
logger.propagate = False  # Prevent propagation to root logger to avoid duplicates
logger.addHandler(logging.NullHandler())  # Prevent "No handlers found" warning

# Global instance to use everywhere
log_print = LogPrinter(logger)
