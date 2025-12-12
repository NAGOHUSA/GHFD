import json
import pandas as pd
import logging
import sqlite3
from datetime import datetime, timedelta
from scraper import GeorgiaPropertyScraper
from analyzer import FlipAnalyzer
from exporter import InvestorExporter
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    with open('pipeline.json', 'r') as f:
        pipeline_config = json.load(f)
    with open('counties.json', 'r') as f:
        counties_config = json.load(f)
    return pipeline_config, counties_config

def scrape_all_counties(pipeline_config, counties_config):
    """Scrape data from all enabled counties"""
    
    # Use default counties or all enabled counties
    if pipeline_config.get('scrape_all_enabled', False):
        counties_to_scrape = [
            county_id for county_id, config in counties_config['counties'].items() 
            if config.get('enabled', False)
        ]
    else:
        counties_to_scrape = pipeline_config.get('default_counties', [])
    
    all_data = []
    
    for county_id in counties_to_scrape:
        if county_id in counties_config['counties']:
            county_info = counties_config['counties'][county_id]
            logger.info(f"Scraping {county_info['name']}")
            
            scraper = GeorgiaPropertyScraper(county_info)
            
            # Generate both buy and sell transactions for flips
            buy_data = scraper.generate_buy_transactions(days_back=180)
            sell_data = scraper.generate_sell_transactions(buy_data)
            
            county_data = pd.concat([buy_data, sell_data], ignore_index=True)
            all_data.append(county_data)
            
            logger.info(f"  Found {len(buy_data)} buy transactions")
            logger.info(f"  Found {len(sell_data)} sell transactions")
        else:
            logger.warning(f"County {county_id} not found in configuration")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def main():
    # Load configuration
    pipeline_config, counties_config = load_config()
    
    # Create necessary directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/results', exist_ok=True)
    
    # Scrape data from counties
    logger.info("Starting data collection...")
    all_properties = scrape_all_counties(pipeline_config, counties_config)
    
    if all_properties.empty:
        logger.error("No data collected!")
        return
    
    logger.info(f"Total properties collected: {len(all_properties)}")
    
    # Save raw data
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_file = f'data/raw/properties_{timestamp}.csv'
    all_properties.to_csv(raw_file, index=False)
    logger.info(f"Raw data saved to {raw_file}")
    
    # Analyze for flips
    logger.info("Analyzing for potential flips...")
    analyzer = FlipAnalyzer()
    flips = analyzer.identify_flips(all_properties)
    
    if flips.empty:
        logger.warning("No flips found!")
    else:
        logger.info(f"Found {len(flips)} potential flips")
        
        # Save flips data
        flips_file = f'data/results/flips_{timestamp}.csv'
        flips.to_csv(flips_file, index=False)
        logger.info(f"Flips saved to {flips_file}")
        
        # Analyze investors
        investor_stats = analyzer.analyze_investors(flips)
        investors_file = f'data/results/investors_{timestamp}.csv'
        investor_stats.to_csv(investors_file, index=False)
        logger.info(f"Investor stats saved to {investors_file}")
        
        # Generate outreach list
        exporter = InvestorExporter()
        outreach_file = exporter.generate_contact_list(
            investor_stats, 
            flips, 
            output_format='csv'
        )
        logger.info(f"Outreach list generated: {outreach_file}")
    
    # Save to database if configured
    if pipeline_config.get('database', {}).get('path'):
        db_path = pipeline_config['database']['path']
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        all_properties.to_sql('properties', conn, if_exists='replace', index=False)
        if not flips.empty:
            flips.to_sql('flips', conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Data saved to database: {db_path}")
    
    # Generate summary report
    generate_summary_report(all_properties, flips, timestamp)

def generate_summary_report(all_properties, flips, timestamp):
    """Generate a summary report of the data collected"""
    summary = {
        'timestamp': timestamp,
        'total_properties': len(all_properties),
        'unique_counties': all_properties['county'].nunique() if 'county' in all_properties.columns else 0,
        'date_range': {
            'min_date': all_properties['sale_date'].min() if 'sale_date' in all_properties.columns else None,
            'max_date': all_properties['sale_date'].max() if 'sale_date' in all_properties.columns else None
        },
        'flips_found': len(flips) if not flips.empty else 0,
        'top_investors': []
    }
    
    if not flips.empty:
        summary['top_investors'] = flips['buyer'].value_counts().head(5).to_dict()
    
    summary_file = f'data/results/summary_{timestamp}.json'
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info(f"Summary report saved to {summary_file}")
    return summary

if __name__ == "__main__":
    main()
