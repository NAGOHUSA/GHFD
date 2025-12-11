#!/usr/bin/env python3
"""
Main pipeline runner for Georgia House Flip Detection
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scraper import MultiCountyScraper
from src.analyzer import FlipAnalyzer
from src.exporter import InvestorExporter
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run the complete pipeline"""
    logger.info("Starting Georgia House Flip Detection Pipeline")
    
    # Step 1: Scrape data
    logger.info("Step 1: Scraping county data...")
    scraper = MultiCountyScraper()
    raw_data = scraper.scrape_all_counties()
    
    if raw_data.empty:
        logger.warning("No data scraped. Using sample data.")
        # Load sample data for demonstration
        raw_data = pd.read_csv('data/sample/sample_properties.csv')
    
    # Save raw data
    raw_data.to_csv('data/raw/latest_scrape.csv', index=False)
    
    # Step 2: Analyze for flips
    logger.info("Step 2: Analyzing for flips...")
    analyzer = FlipAnalyzer()
    
    # If we have a database, load from there
    # Otherwise use the scraped data
    try:
        data = analyzer.load_from_database()
        if data.empty:
            data = raw_data
    except:
        data = raw_data
    
    flips = analyzer.identify_flips(data)
    
    if flips.empty:
        logger.warning("No flips identified. Try adjusting criteria.")
        return
    
    logger.info(f"Identified {len(flips)} potential flips")
    
    # Step 3: Analyze investors
    logger.info("Step 3: Analyzing investors...")
    investors = analyzer.analyze_investors(flips)
    
    # Step 4: Generate report
    logger.info("Step 4: Generating reports...")
    report = analyzer.generate_report(flips, investors)
    
    # Step 5: Export investor contacts
    logger.info("Step 5: Exporting investor contacts...")
    exporter = InvestorExporter()
    export_file = exporter.generate_contact_list(investors, flips, 'excel')
    
    # Display summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE - SUMMARY")
    print("="*60)
    print(f"Total Flips Identified: {len(flips)}")
    print(f"Total Investors Found: {len(investors)}")
    print(f"Total Profit Detected: ${report['total_profit']:,.2f}")
    print(f"Average Hold Period: {report['avg_hold_days']:.1f} days")
    print(f"Average ROI: {report['avg_roi']:.1f}%")
    print("\nTop 5 Investors:")
    print(investors.head().to_string())
    print("\n" + "="*60)
    print(f"Reports saved to: data/results/")
    print(f"Investor contacts: {export_file}.xlsx")
    print("="*60)

if __name__ == "__main__":
    main()
