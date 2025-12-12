import sys
import os
import json
import pandas as pd
import logging
import sqlite3
from datetime import datetime
import time

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Now import from src
from scraper import GeorgiaPropertyScraper
from analyzer import FlipAnalyzer
from exporter import InvestorExporter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration files"""
    with open('../pipeline.json', 'r') as f:
        pipeline_config = json.load(f)
    
    with open('../counties.json', 'r') as f:
        counties_config = json.load(f)
    
    return pipeline_config, counties_config

def scrape_counties(pipeline_config, counties_config):
    """Scrape data from configured counties"""
    
    # Get which counties to scrape
    if pipeline_config.get('scrape_all_enabled', False):
        counties_to_scrape = [
            county_id for county_id, config in counties_config['counties'].items() 
            if config.get('enabled', True)
        ]
    else:
        counties_to_scrape = pipeline_config.get('default_counties', [])
    
    # LIMIT counties for testing - remove or increase this for production
    max_counties = pipeline_config.get('max_counties_per_run', 20)
    if len(counties_to_scrape) > max_counties:
        logger.info(f"Limiting to {max_counties} counties for performance (out of {len(counties_to_scrape)})")
        counties_to_scrape = counties_to_scrape[:max_counties]
    
    logger.info(f"Scraping {len(counties_to_scrape)} counties")
    
    all_data = []
    
    for county_id in counties_to_scrape:
        if county_id in counties_config['counties']:
            county_info = counties_config['counties'][county_id]
            scraper = GeorgiaPropertyScraper(county_info)
            
            # Get data for this county
            start_time = time.time()
            county_data = scraper.get_recent_sales(
                days_back=pipeline_config.get('default_days_back', 180)
            )
            elapsed = time.time() - start_time
            
            if not county_data.empty:
                all_data.append(county_data)
                logger.info(f"  {county_info['name']}: {len(county_data)} transactions ({elapsed:.2f}s)")
            else:
                logger.warning(f"  {county_info['name']}: No data generated")
        else:
            logger.error(f"County {county_id} not found in configuration")
    
    # Combine all county data
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total transactions collected: {len(combined_data)}")
        return combined_data
    
    return pd.DataFrame()

def save_data(data, filename_prefix, output_dir='../data'):
    """Save data to multiple formats"""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    files = []
    
    # CSV
    csv_file = f"{output_dir}/{filename_prefix}_{timestamp}.csv"
    data.to_csv(csv_file, index=False)
    files.append(csv_file)
    
    return files

def analyze_flips(properties_df):
    """Analyze properties to find flips"""
    analyzer = FlipAnalyzer()
    
    # Identify flips with timing
    start_time = time.time()
    flips_df = analyzer.identify_flips(properties_df)
    analysis_time = time.time() - start_time
    
    if flips_df.empty:
        logger.warning(f"No flips found! Analysis took {analysis_time:.2f}s")
        return pd.DataFrame(), pd.DataFrame()
    
    logger.info(f"Found {len(flips_df)} potential flips in {analysis_time:.2f}s")
    
    # Analyze investors
    investors_df = analyzer.analyze_investors(flips_df)
    logger.info(f"Found {len(investors_df)} active investors")
    
    return flips_df, investors_df

def generate_dashboard_data(flips_df, investors_df):
    """Generate data formatted for dashboard"""
    # This would be used by your dashboard
    dashboard_data = {
        'summary': {
            'total_flips': len(flips_df) if not flips_df.empty else 0,
            'total_investors': len(investors_df) if not investors_df.empty else 0,
            'total_profit': flips_df['profit'].sum() if not flips_df.empty else 0,
            'avg_roi': flips_df['roi'].mean() if not flips_df.empty else 0
        },
        'top_investors': investors_df.head(10).to_dict('records') if not investors_df.empty else [],
        'recent_flips': flips_df.head(50).to_dict('records') if not flips_df.empty else []
    }
    
    return dashboard_data

def main():
    """Main pipeline execution"""
    logger.info("Starting Georgia House Flip Pipeline")
    
    # Load configuration
    pipeline_config, counties_config = load_config()
    
    # Create directories
    os.makedirs('../data/raw', exist_ok=True)
    os.makedirs('../data/results', exist_ok=True)
    os.makedirs('../data/dashboard', exist_ok=True)
    
    # Step 1: Scrape data
    logger.info("Step 1: Collecting property data")
    properties_df = scrape_counties(pipeline_config, counties_config)
    
    if properties_df.empty:
        logger.error("No data collected. Exiting.")
        return
    
    # Save raw data
    raw_files = save_data(properties_df, 'raw/properties', '../data')
    logger.info(f"Raw data saved to: {raw_files[0]}")
    
    # Step 2: Analyze for flips
    logger.info("Step 2: Analyzing for property flips")
    flips_df, investors_df = analyze_flips(properties_df)
    
    if flips_df.empty:
        logger.warning("No flips found. Generating sample outreach data.")
        # Generate sample data for testing
        flips_df = pd.DataFrame([{
            'property_id': 'F000001',
            'address': '123 Main St, Atlanta, GA 30303',
            'buy_date': '2023-01-15',
            'buy_price': 250000,
            'buyer': 'Atlanta Flip Masters LLC',
            'sell_date': '2023-06-15',
            'sell_price': 350000,
            'seller': 'Atlanta Flip Masters LLC',
            'hold_days': 150,
            'profit': 100000,
            'roi': 40.0,
            'county': 'Fulton County'
        }])
        
        investors_df = pd.DataFrame([{
            'investor_name': 'Atlanta Flip Masters LLC',
            'total_flips': 5,
            'total_profit': 500000,
            'avg_profit_per_flip': 100000,
            'avg_hold_days': 120,
            'avg_roi': 35.0
        }])
    
    # Save flip analysis results
    if not flips_df.empty:
        flip_files = save_data(flips_df, 'results/flips', '../data')
        logger.info(f"Flip analysis saved to: {flip_files[0]}")
    
    if not investors_df.empty:
        investor_files = save_data(investors_df, 'results/investors', '../data')
        logger.info(f"Investor analysis saved to: {investor_files[0]}")
    
    # Step 3: Generate outreach materials
    logger.info("Step 3: Generating outreach materials")
    exporter = InvestorExporter()
    
    if not investors_df.empty and not flips_df.empty:
        contact_file = exporter.generate_contact_list(
            investors_df, 
            flips_df,
            output_format='csv'
        )
        logger.info(f"Contact list generated: {contact_file}")
    
    # Step 4: Generate dashboard data
    logger.info("Step 4: Generating dashboard data")
    dashboard_data = generate_dashboard_data(flips_df, investors_df)
    
    dashboard_file = '../data/dashboard/dashboard_data.json'
    with open(dashboard_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2)
    
    logger.info(f"Dashboard data saved to: {dashboard_file}")
    
    # Step 5: Save to database
    if pipeline_config.get('database', {}).get('path'):
        db_path = pipeline_config['database']['path']
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        properties_df.to_sql('properties', conn, if_exists='replace', index=False)
        
        if not flips_df.empty:
            flips_df.to_sql('flips', conn, if_exists='replace', index=False)
        
        if not investors_df.empty:
            investors_df.to_sql('investors', conn, if_exists='replace', index=False)
        
        conn.close()
        logger.info(f"Data saved to database: {db_path}")
    
    # Generate summary report
    generate_summary_report(properties_df, flips_df, investors_df)
    
    logger.info("Pipeline completed successfully!")

def generate_summary_report(properties_df, flips_df, investors_df):
    """Generate a summary report of the pipeline run"""
    summary = {
        'timestamp': datetime.now().isoformat(),
        'pipeline_version': '1.0.0',
        'data_summary': {
            'total_transactions': len(properties_df),
            'unique_counties': properties_df['county'].nunique() if 'county' in properties_df.columns else 0,
            'date_range': {
                'min_date': properties_df['sale_date'].min() if 'sale_date' in properties_df.columns else None,
                'max_date': properties_df['sale_date'].max() if 'sale_date' in properties_df.columns else None
            }
        },
        'flips_summary': {
            'total_flips': len(flips_df) if not flips_df.empty else 0,
            'total_profit': flips_df['profit'].sum() if not flips_df.empty else 0,
            'avg_profit': flips_df['profit'].mean() if not flips_df.empty else 0,
            'avg_hold_days': flips_df['hold_days'].mean() if not flips_df.empty else 0
        },
        'investors_summary': {
            'total_investors': len(investors_df) if not investors_df.empty else 0,
            'top_5_investors': investors_df.head(5).to_dict('records') if not investors_df.empty else []
        }
    }
    
    summary_file = f"../data/results/pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Summary report saved to: {summary_file}")
    
    # Print quick summary to console
    print("\n" + "="*50)
    print("PIPELINE SUMMARY")
    print("="*50)
    print(f"Total Transactions: {summary['data_summary']['total_transactions']}")
    print(f"Total Flips Found: {summary['flips_summary']['total_flips']}")
    print(f"Total Investors: {summary['investors_summary']['total_investors']}")
    if not flips_df.empty:
        print(f"Total Profit: ${summary['flips_summary']['total_profit']:,.0f}")
    print("="*50)

if __name__ == "__main__":
    main()
