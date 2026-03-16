#!/usr/bin/env python3
import os
import sys

# -------------------------------------------------------------------------
# Ensure the external project root (/genomefltr) is importable
# -------------------------------------------------------------------------
EXTERNAL_DIR = "/genomefltr"
if EXTERNAL_DIR not in sys.path:
    sys.path.insert(0, EXTERNAL_DIR)

# -------------------------------------------------------------------------
# Add the application directory (safe and explicit)
# -------------------------------------------------------------------------
APP_DIR = "/var/www/vhosts/genomefltr.tau.ac.il/httpdocs"
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# -------------------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------------------
os.environ['SECRET_KEY'] = 'dAGBnn!gqr@TBu4bc1WMsq4c5AcmCdGk6eX@CR!UERu'
os.environ['PASSPHRASE_KILL'] = 'uXLwdwNFV6KZRLdHcSTdhZA4w'
os.environ['PASSPHRASE_CLEAN'] = 'P9SEytcgbd4FUsRNSycpdzzzh'

# -------------------------------------------------------------------------
# Import Flask application
# -------------------------------------------------------------------------
from app import app as application

