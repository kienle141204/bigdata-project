#!/usr/bin/env python3
"""
Quick script to test UI accessibility
Tests both Kafka UI and Spark UI
"""

import time
import requests
import sys
from loguru import logger

def test_ui(url, name, max_retries=10, wait_seconds=3):
    """Test if UI is accessible."""
    logger.info(f"Testing {name}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logger.success(f"✅ {name} is accessible at {url}")
                return True
            else:
                logger.warning(f"⚠️  {name} returned status {response.status_code}")
        except requests.exceptions.ConnectionError:
            if attempt < max_retries:
                logger.info(f"   Attempt {attempt}/{max_retries} - Waiting {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                logger.error(f"❌ {name} not accessible at {url}")
                return False
        except Exception as e:
            logger.error(f"❌ Error testing {name}: {e}")
            return False
    
    return False

def main():
    """Test all UIs."""
    logger.info("🎯 Testing UI Accessibility\n")
    
    results = {}
    
    # Test Kafka UI
    results['Kafka UI'] = test_ui(
        'http://localhost:8080',
        'Kafka UI',
        max_retries=5,
        wait_seconds=2
    )
    
    print()
    
    # Test Spark Streaming UI
    results['Spark UI'] = test_ui(
        'http://localhost:4040',
        'Spark Streaming UI',
        max_retries=10,
        wait_seconds=3
    )
    
    print("\n" + "="*60)
    logger.info("📊 Test Results Summary:")
    print("="*60)
    
    for name, success in results.items():
        status = "✅ ACCESSIBLE" if success else "❌ NOT ACCESSIBLE"
        logger.info(f"{name:20s}: {status}")
    
    print("="*60)
    
    if all(results.values()):
        logger.success("\n🎉 All UIs are accessible!")
        logger.info("\nYou can now open:")
        logger.info("  • Kafka UI:  http://localhost:8080")
        logger.info("  • Spark UI:  http://localhost:4040")
        return 0
    else:
        logger.error("\n⚠️  Some UIs are not accessible.")
        logger.info("\nTroubleshooting:")
        
        if not results['Kafka UI']:
            logger.info("  • Kafka UI: Run 'docker-compose up -d kafka-ui'")
        
        if not results['Spark UI']:
            logger.info("  • Spark UI: Run 'docker-compose up -d streaming-etl'")
            logger.info("  • Wait 30 seconds and try again")
        
        return 1

if __name__ == "__main__":
    sys.exit(main())
