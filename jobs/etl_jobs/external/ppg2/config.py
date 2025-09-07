from typing import List, Dict, Optional, Any
from enum import Enum
from utils.file_utils import slugify

import os
from pathlib import Path




######## base configs
GCP_PROJECT = os.environ.get("PROJECT_NAME", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"

REGION_HIERARCHY_TABLE = "region_department"


