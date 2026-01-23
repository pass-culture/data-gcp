import logging

import typer


class CLIState:
    """Global CLI state to track flags."""

    def __init__(self):
        self.verbose: bool = False
        self.silent: bool = False

    def set_flags(self, verbose: bool, silent: bool):
        self.verbose = verbose
        self.silent = silent
        # Update the global logger level
        logger.setLevel(logging.INFO if verbose else logging.WARNING)


state = CLIState()


class LogPrinter:
    """Logger + Typer print helper."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def set_verbose(self, verbose: bool):
        """Proxy to global state (legacy support)."""
        state.set_flags(verbose, state.silent)

    def set_flags(self, verbose: bool, silent: bool):
        """Proxy to global state."""
        state.set_flags(verbose, silent)

    def debug(self, msg: str, **style):
        self.logger.debug(msg)
        if not state.silent and self.logger.isEnabledFor(logging.DEBUG):
            typer.secho(msg, **style)

    def info(self, msg: str, **style):
        self.logger.info(msg)
        # Progress info should always show in CLI unless --quiet is used
        if not state.silent:
            fg = style.pop("fg", "cyan")
            typer.secho(msg, fg=fg, **style)

    def warning(self, msg: str, **style):
        self.logger.warning(msg)
        if not state.silent:
            fg = style.pop("fg", "yellow")
            typer.secho(msg, fg=fg, **style)

    def error(self, msg: str, **style):
        self.logger.error(msg)
        # Errors should always be visible in the console
        fg = style.pop("fg", "red")
        typer.secho(msg, fg=fg, **style)

    def critical(self, msg: str, **style):
        self.logger.critical(msg)
        # Critical errors should always be visible in the console
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
