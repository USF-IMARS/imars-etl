import logging
import os

from imars_etl.filepath.data import valid_pattern_vars
from imars_etl.filepath.parse import filename_matches_pattern

check_match = filename_matches_pattern
