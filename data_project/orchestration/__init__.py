import sys
from pathlib import Path

# Add project root to path to handle working directory changes by dagster-dg-cli
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
