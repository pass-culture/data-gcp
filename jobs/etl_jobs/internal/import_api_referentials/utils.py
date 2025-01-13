import importlib
import sys
from unittest.mock import MagicMock

def import_with_auto_mock(module_name):
    """
    Dynamically imports a module and automatically mocks any missing dependencies.

    Args:
        module_name (str): The name of the module to import.

    Returns:
        Module: The imported module object.
    """
    original_modules = {}

    def handle_missing_module(name):
        """Mock a missing module and track it."""
        if name not in sys.modules:
            sys.modules[name] = MagicMock()
            original_modules[name] = None

    try:
        # Attempt to import the module
        return importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        # Extract the name of the missing module
        missing_module = str(e).split("'")[1]
        handle_missing_module(missing_module)
        # Retry importing the module
        return import_with_auto_mock(module_name)
    finally:
        # Restore original sys.modules entries
        for mod, original in original_modules.items():
            if original is None:
                del sys.modules[mod]
            else:
                sys.modules[mod] = original
