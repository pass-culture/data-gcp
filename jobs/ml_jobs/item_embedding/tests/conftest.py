import sys
from pathlib import Path

# Add the parent directory so tests can import project modules directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
