#!/usr/bin/env python3
"""
Quick manual runner for the pipeline
Usage: python manual_run.py [options]
"""

import subprocess
import sys
import os
from datetime import datetime

def run_pipeline(counties='all', days_back=180, dry_run=False):
    """Run the pipeline with given parameters"""
    cmd = [
        sys.executable, 'scripts/run_pipeline.py',
        '--counties', counties,
        '--days-back', str(days_back),
        '--verbose'
    ]
    
    if dry_run:
        cmd.append('--dry-run=true')
    
    print(f"Running: {' '.join(cmd)}")
    print(f"Started at: {datetime.now()}")
    print("-" * 60)
    
    result = subprocess.run(cmd, capture_output=False)
    
    print("-" * 60)
    print(f"Finished at: {datetime.now()}")
    print(f"Exit code: {result.returncode}")
    
    return result.returncode

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Manual pipeline runner')
    parser.add_argument('--counties', default='all', help='Counties to process')
    parser.add_argument('--days-back', type=int, default=180, help='Days of history')
    parser.add_argument('--dry-run', action='store_true', help='Test run')
    parser.add_argument('--quick', action='store_true', help='Quick test (7 days, dry run)')
    
    args = parser.parse_args()
    
    if args.quick:
        exit_code = run_pipeline(counties='fulton', days_back=7, dry_run=True)
    else:
        exit_code = run_pipeline(
            counties=args.counties,
            days_back=args.days_back,
            dry_run=args.dry_run
        )
    
    sys.exit(exit_code)
