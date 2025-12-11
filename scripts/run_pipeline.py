#!/usr/bin/env python3
"""
Main pipeline runner for Georgia House Flip Detection
Optimized for GitHub Actions
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
import json
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scraper import MultiCountyScraper
from src.analyzer import FlipAnalyzer
from src.exporter import InvestorExporter

def setup_logging():
    """Configure logging for GitHub Actions"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/pipeline.log')
        ]
    )
    return logging.getLogger(__name__)

def ensure_directories():
    """Ensure all required directories exist"""
    directories = [
        'data/raw',
        'data/processed',
        'data/results',
        'data/exports',
        'logs',
        'config'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def generate_static_data_files(flips_df, investors_df, stats):
    """Generate static JSON files for the dashboard"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Generate dashboard_data.json (main data file)
    dashboard_data = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "total_flips": len(flips_df),
            "total_investors": len(investors_df),
            "data_source": "Georgia County Public Records"
        },
        "stats": {
            "total_flips_identified": stats.get('total_flips_identified', 0),
            "total_investors": stats.get('total_investors', 0),
            "total_profit": stats.get('total_profit', 0),
            "avg_hold_days": stats.get('avg_hold_days', 0),
            "avg_roi": stats.get('avg_roi', 0),
            "by_county": stats.get('by_county', {}),
            "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        "recent_flips": flips_df.head(100).to_dict('records') if not flips_df.empty else [],
        "top_investors": investors_df.head(50).to_dict('records') if not investors_df.empty else []
    }
    
    # Save main dashboard data
    with open('data/dashboard_data.json', 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    
    # 2. Save individual files for granular access
    if not flips_df.empty:
        flips_df.to_json('data/flips.json', orient='records', indent=2)
        flips_df.to_csv('data/flips.csv', index=False)
    
    if not investors_df.empty:
        investors_df.to_json('data/investors.json', orient='records', indent=2)
        investors_df.to_csv('data/investors.csv', index=False)
    
    # 3. Save timestamped copies
    flips_df.to_json(f'data/results/flips_{timestamp}.json', orient='records')
    investors_df.to_json(f'data/results/investors_{timestamp}.json', orient='records')
    
    # 4. Create a README file for GitHub
    readme_content = f"""# Georgia House Flip Data

## Latest Run
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Total Flips**: {len(flips_df)}
- **Total Investors**: {len(investors_df)}
- **Total Profit**: ${stats.get('total_profit', 0):,.2f}

## Files
- `dashboard_data.json` - Complete data for the dashboard
- `flips.json` - All detected flips
- `investors.json` - All identified investors
- `flips.csv` - Flips in CSV format
- `investors.csv` - Investors in CSV format

## Data Source
Scraped from Georgia county public records through qPublic system.
"""
    
    with open('data/README.md', 'w') as f:
        f.write(readme_content)

def main():
    """Run the complete pipeline optimized for GitHub"""
    ensure_directories()
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("Georgia House Flip Detection Pipeline - GitHub Edition")
    logger.info(f"Run started: {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        # Step 1: Scrape data from counties
        logger.info("Step 1: Scraping county data...")
        scraper = MultiCountyScraper()
        raw_data = scraper.scrape_all_counties()
        
        if raw_data.empty:
            logger.warning("No data scraped. Checking for existing database...")
            # Try to load from existing database
            analyzer = FlipAnalyzer()
            db_data = analyzer.load_from_database()
            
            if not db_data.empty:
                raw_data = db_data
                logger.info(f"Loaded {len(raw_data)} records from database")
            else:
                logger.error("No data available. Pipeline cannot continue.")
                return 1
        
        # Step 2: Analyze for flips
        logger.info("Step 2: Analyzing for flips...")
        analyzer = FlipAnalyzer()
        flips = analyzer.identify_flips(raw_data)
        
        if flips.empty:
            logger.warning("No flips identified with current criteria.")
            # Create empty data structure for dashboard
            flips = pd.DataFrame(columns=[
                'property_id', 'address', 'buy_date', 'buy_price', 'buyer',
                'sell_date', 'sell_price', 'seller', 'hold_days', 'profit',
                'roi', 'county'
            ])
        
        logger.info(f"Identified {len(flips)} potential flips")
        
        # Step 3: Analyze investors
        logger.info("Step 3: Analyzing investors...")
        investors = analyzer.analyze_investors(flips)
        
        if investors.empty:
            investors = pd.DataFrame(columns=[
                'investor_name', 'total_flips', 'total_profit',
                'avg_profit_per_flip', 'avg_hold_days', 'avg_roi'
            ])
        
        # Step 4: Generate stats
        logger.info("Step 4: Generating statistics...")
        stats = {
            'total_flips_identified': len(flips),
            'total_investors': len(investors),
            'total_profit': float(flips['profit'].sum()) if not flips.empty else 0,
            'avg_hold_days': float(flips['hold_days'].mean()) if not flips.empty else 0,
            'avg_roi': float(flips['roi'].mean()) if not flips.empty else 0,
            'by_county': flips['county'].value_counts().to_dict() if not flips.empty else {},
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Step 5: Generate static files for dashboard
        logger.info("Step 5: Generating static data files...")
        generate_static_data_files(flips, investors, stats)
        
        # Step 6: Export investor contacts
        logger.info("Step 6: Exporting investor contacts...")
        exporter = InvestorExporter()
        exporter.generate_contact_list(investors, flips, 'csv')
        
        # Step 7: Final summary
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total Flips: {len(flips)}")
        logger.info(f"Total Investors: {len(investors)}")
        logger.info(f"Total Profit: ${stats['total_profit']:,.2f}")
        logger.info(f"Files generated:")
        logger.info(f"  - data/dashboard_data.json")
        logger.info(f"  - data/flips.json")
        logger.info(f"  - data/investors.json")
        logger.info(f"  - data/flips.csv")
        logger.info(f"  - data/investors.csv")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
