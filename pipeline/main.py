"""
Main Pipeline Orchestrator
Run full pipeline: Crawl -> Bronze -> Silver -> Gold
"""

import argparse
import subprocess
from datetime import datetime
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_step(name: str, command: list, description: str):
    """Run a single pipeline step"""
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STEP: {name}")
    print(f"Description: {description}")
    print(f"Command: {' '.join(command)}")
    print('='*60)
    
    result = subprocess.run(command, cwd=PROJECT_ROOT)
    
    if result.returncode != 0:
        print(f"❌ Step '{name}' failed with code {result.returncode}")
        return False
    
    print(f"✅ Step '{name}' completed successfully")
    return True


def run_crawlers(max_pages: int = 10, max_items: int = None):
    """
    Step 1: Crawl data from TopCV
    Output: Bronze layer (raw/jobs/)
    """
    cmd = ["scrapy", "crawl", "regular_job_crawler"]
    if max_items:
        cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={max_items}"])
    
    return run_step("CRAWL_DATA", cmd, f"Crawl job data from TopCV (max {max_pages} pages)")


def run_bronze_to_silver(date: str = None, limit: int = None):
    """
    Step 2: Clean data from Bronze -> Silver
    Input: Bronze layer (raw/jobs/)
    Output: Silver layer (processed/jobs/)
    """
    cmd = ["python", "-m", "pipeline.data_cleaning"]
    if date:
        cmd.extend(["--date", date])
    if limit:
        cmd.extend(["--limit", str(limit)])
    
    return run_step("BRONZE_TO_SILVER", cmd, "Parse HTML and extract structured data")


def run_silver_to_gold():
    """
    Step 3: Aggregate data from Silver -> Gold
    Input: Silver layer (processed/jobs/)
    Output: Gold layer (curated/)
    """
    print("\n⚠️ Silver to Gold pipeline not implemented yet")
    return True


def main():
    parser = argparse.ArgumentParser(description="Job Data Pipeline Orchestrator")
    
    parser.add_argument("--step", choices=["crawl", "bronze-to-silver", "silver-to-gold", "all"],
                        default="all", help="Which step(s) to run")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages to crawl")
    parser.add_argument("--max-items", type=int, help="Max items to crawl")
    parser.add_argument("--date", type=str, help="Date to process (YYYY/MM/DD)")
    parser.add_argument("--limit", type=int, help="Limit records to process")
    
    args = parser.parse_args()
    
    print(f"\n{'#'*60}")
    print(f"# JOB DATA PIPELINE")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Step: {args.step}")
    print(f"{'#'*60}")
    
    success = True
    
    if args.step in ["crawl", "all"]:
        success = run_crawlers(max_pages=args.max_pages, max_items=args.max_items)
        if not success and args.step == "all":
            return
    
    if args.step in ["bronze-to-silver", "all"]:
        success = run_bronze_to_silver(date=args.date, limit=args.limit)
        if not success and args.step == "all":
            return
    
    if args.step in ["silver-to-gold", "all"]:
        success = run_silver_to_gold()
    
    print(f"\n{'#'*60}")
    print(f"# PIPELINE COMPLETED")
    print(f"# Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Status: {'SUCCESS ✅' if success else 'FAILED ❌'}")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
