"""Allow ``python -m pipeline <command>``."""

import sys

from pipeline.cli import main

sys.exit(main())
