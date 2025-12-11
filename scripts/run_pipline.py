#!/usr/bin/env python3
"""
Main pipeline runner for Georgia House Flip Detection
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scraper import MultiCountyScraper
from src.analyzer import FlipAnalyzer
from src.exporter import InvestorExporter

def setup_logging(log_file=None):
    """Configure logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler()]
    
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=handlers
    )
    
    return logging.getLogger(__name__)

def load_config(config_path='config/pipeline.json'):
    """Load pipeline configuration"""
    default_config = {
        "default_counties": ["fulton", "gwinnett", "cobb"],
        "default_days_back": 180,
        "max_scrape_records": 10000,
        "notification_email": "",
        "data_retention_days": 90
    }
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            # Merge with defaults
            return {**default_config, **config}
    except FileNotFoundError:
        logging.warning(f"Config file {config_path} not found, using defaults")
        return default_config

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Georgia House Flip Detection Pipeline'
    )
    
    parser.add_argument(
        '--counties',
        type=str,
        default='all',
        help='Comma-separated list of counties or "all" (default: all)'
    )
    
    parser.add_argument(
        '--days-back',
        type=int,
        default=180,
        help='Number of days of historical data to fetch (default: 180)'
    )
    
    parser.add_argument(
        '--dry-run',
        type=lambda x: x.lower() == 'true',
        default=False,
        help='Test run without saving to database (default: false)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/results',
        help='Output directory for results (default: data/results)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Log file path (default: no file logging)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()

def cleanup_old_files(directory, days_old=90):
    """Remove files older than specified days"""
    try:
        from datetime import datetime
        import os
        import time
        
        cutoff = time.time() - (days_old * 86400)
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                filepath = os.path.join(root, file)
                if os.path.getmtime(filepath) < cutoff:
                    os.remove(filepath)
                    logging.info(f"Removed old file: {filepath}")
    except Exception as e:
        logging.warning(f"Cleanup failed: {e}")

def main():
    """Run the complete pipeline"""
    args = parse_arguments()
    config = load_config()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 60)
    logger.info("Starting Georgia House Flip Detection Pipeline")
    logger.info(f"Run started at: {datetime.now()}")
    logger.info(f"Arguments: {vars(args)}")
    logger.info("=" * 60)
    
    try:
        # Parse counties
        if args.counties.lower() == 'all':
            counties_to_run = config['default_counties']
        else:
            counties_to_run = [c.strip() for c in args.counties.split(',')]
        
        logger.info(f"Processing counties: {', '.join(counties_to_run)}")
        
        # Step 1: Configure scraper with selected counties
        logger.info("Step 1: Configuring scraper...")
        
        # Load counties config and enable only selected ones
        with open('config/counties.json', 'r') as f:
            counties_config = json.load(f)
        
        for county in counties_config['counties']:
            counties_config['counties'][county]['enabled'] = county in counties_to_run
        
        # Save modified config for scraper
        temp_config_path = 'config/counties_temp.json'
        with open(temp_config_path, 'w') as f:
            json.dump(counties_config, f, indent=2)
        
        # Step 2: Scrape data
        logger.info("Step 2: Scraping county data...")
        scraper = MultiCountyScraper(temp_config_path)
        
        if args.dry_run:
            logger.info("DRY RUN: Skipping actual scraping")
            # Load sample data for demonstration
            import pandas as pd
            raw_data = pd.read_csv('data/sample/sample_properties.csv')
        else:
            raw_data = scraper.scrape_all_counties()
        
        if raw_data.empty:
            logger.warning("No data scraped. Using sample data.")
            raw_data = pd.read_csv('data/sample/sample_properties.csv')
        
        # Save raw data with timestamp
        os.makedirs('data/raw', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        raw_data.to_csv(f'data/raw/scrape_{timestamp}.csv', index=False)
        logger.info(f"Saved raw data: data/raw/scrape_{timestamp}.csv")
        
        # Step 3: Analyze for flips
        logger.info("Step 3: Analyzing for flips...")
        analyzer = FlipAnalyzer()
        
        if not args.dry_run:
            # Save to database
            scraper.scrapers[counties_to_run[0]].save_to_database(raw_data)
            data = analyzer.load_from_database()
        else:
            data = raw_data
        
        flips = analyzer.identify_flips(data)
        
        if flips.empty:
            logger.warning("No flips identified. Try adjusting criteria.")
            return 0
        
        logger.info(f"Identified {len(flips)} potential flips")
        
        # Step 4: Analyze investors
        logger.info("Step 4: Analyzing investors...")
        investors = analyzer.analyze_investors(flips)
        
        # Step 5: Generate report
        logger.info("Step 5: Generating reports...")
        report = analyzer.generate_report(flips, investors)
        
        # Step 6: Export investor contacts
        logger.info("Step 6: Exporting investor contacts...")
        exporter = InvestorExporter()
        export_file = exporter.generate_contact_list(investors, flips, 'excel')
        
        # Step 7: Generate summary statistics
        logger.info("Step 7: Generating summary...")
        
        # Calculate additional statistics
        stats = {
            'run_timestamp': datetime.now().isoformat(),
            'counties_processed': counties_to_run,
            'days_back': args.days_back,
            'total_flips': len(flips),
            'total_investors': len(investors),
            'total_profit': float(flips['profit'].sum()),
            'avg_hold_days': float(flips['hold_days'].mean()),
            'avg_roi': float(flips['roi'].mean()),
            'top_county': flips['county'].mode().iloc[0] if not flips.empty else 'None',
            'top_investor': investors.iloc[0]['investor_name'] if not investors.empty else 'None'
        }
        
        # Save stats
        stats_file = os.path.join(args.output_dir, f'stats_{timestamp}.json')
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        # Step 8: Cleanup old files
        logger.info("Step 8: Cleaning up old files...")
        cleanup_old_files('data/raw', config['data_retention_days'])
        cleanup_old_files('data/results', config['data_retention_days'])
        
        # Display summary
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE - SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Flips Identified: {len(flips)}")
        logger.info(f"Total Investors Found: {len(investors)}")
        logger.info(f"Total Profit Detected: ${stats['total_profit']:,.2f}")
        logger.info(f"Average Hold Period: {stats['avg_hold_days']:.1f} days")
        logger.info(f"Average ROI: {stats['avg_roi']:.1f}%")
        logger.info(f"Top County: {stats['top_county']}")
        logger.info(f"Top Investor: {stats['top_investor']}")
        logger.info("=" * 60)
        logger.info(f"Reports saved to: {args.output_dir}/")
        logger.info(f"Investor contacts: {export_file}.xlsx")
        logger.info(f"Statistics: {stats_file}")
        logger.info("=" * 60)
        
        # Clean up temp config
        if os.path.exists(temp_config_path):
            os.remove(temp_config_path)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
