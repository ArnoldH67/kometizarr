#!/usr/bin/env python3
"""
Test Collection Manager - Dry-run test for decade collections

This script tests the collection manager in dry-run mode (no actual changes).
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.collection_manager import CollectionManager
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_dry_run():
    """Test collection manager in dry-run mode"""

    # ⚠️  REPLACE WITH YOUR ACTUAL PLEX DETAILS
    PLEX_URL = "http://192.168.1.20:32400"
    PLEX_TOKEN = "YOUR_PLEX_TOKEN_HERE"  # Get from homelab_context.md
    LIBRARY_NAME = "Movies"

    logger.info("=== Kometizarr Collection Manager Test (DRY-RUN) ===\n")

    # Initialize manager in dry-run mode
    manager = CollectionManager(
        plex_url=PLEX_URL,
        plex_token=PLEX_TOKEN,
        library_name=LIBRARY_NAME,
        dry_run=True  # IMPORTANT: Dry-run mode for testing
    )

    # Test decade collections
    decades = [
        {"title": "1980s Movies", "start": 1980, "end": 1989},
        {"title": "1990s Movies", "start": 1990, "end": 1999},
        {"title": "2000s Movies", "start": 2000, "end": 2009},
        {"title": "2010s Movies", "start": 2010, "end": 2019},
        {"title": "2020s Movies", "start": 2020, "end": 2029}
    ]

    logger.info("\nTesting Decade Collections (DRY-RUN):\n")
    manager.create_decade_collections(decades)

    # Test studio collections
    studios = [
        {
            "title": "Marvel Cinematic Universe",
            "studios": ["Marvel Studios"]
        },
        {
            "title": "DC Extended Universe",
            "studios": ["DC Comics", "DC Entertainment", "Warner Bros."]
        }
    ]

    logger.info("\nTesting Studio Collections (DRY-RUN):\n")
    manager.create_studio_collections(studios)

    logger.info("\n" + "="*60)
    logger.info("✅ Dry-run test complete!")
    logger.info("="*60)
    logger.info("\nNo changes were made to your Plex library.")
    logger.info("To apply changes, edit the script and set dry_run=False")


if __name__ == '__main__':
    test_dry_run()
