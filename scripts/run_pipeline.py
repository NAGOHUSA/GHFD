#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import datetime
import json
from src.scraper import GeorgiaPropertyScraper
from src.analyzer import FlipAnalyzer

def run_pipeline():
    print("="*60)
    print("Georgia House Flip Pipeline")
    print(f"Started: {datetime.now()}")
    print("="*60)
    
    # Load county configs
    with open('config/counties.json', 'r') as f:
        counties = json.load(f)['counties']
    
    # Scrape all counties
    all_data = []
    
    for county_id, config in counties.items():
        if config.get('enabled', True):
            print(f"Scraping {config['name']}...")
            scraper = GeorgiaPropertyScraper(config)
            data = scraper.get_recent_sales()
            
            if not data.empty:
                all_data.append(data)
                print(f"  Found {len(data)} records")
    
    if not all_data:
        print("No data scraped")
        return
    
    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)
    
    # Analyze for flips
    analyzer = FlipAnalyzer()
    flips = analyzer.identify_flips(combined_data)
    investors = analyzer.analyze_investors(flips)
    
    # Generate stats
    stats = {
        'total_flips_identified': len(flips),
        'total_investors': len(investors),
        'total_profit': float(flips['profit'].sum()) if not flips.empty else 0,
        'avg_hold_days': float(flips['hold_days'].mean()) if not flips.empty else 0,
        'avg_roi': float(flips['roi'].mean()) if not flips.empty else 0,
        'by_county': flips['county'].value_counts().to_dict() if not flips.empty else {},
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Create dashboard data
    dashboard_data = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'total_flips': len(flips),
            'total_investors': len(investors),
            'data_source': 'Georgia County Public Records'
        },
        'stats': stats,
        'recent_flips': flips.head(100).to_dict('records') if not flips.empty else [],
        'top_investors': investors.head(50).to_dict('records') if not investors.empty else []
    }
    
    # Save files
    os.makedirs('data', exist_ok=True)
    
    with open('data/dashboard_data.json', 'w') as f:
        json.dump(dashboard_data, f, indent=2)
    
    if not flips.empty:
        flips.to_csv('data/flips.csv', index=False)
    
    if not investors.empty:
        investors.to_csv('data/investors.csv', index=False)
    
    # Save raw data
    combined_data.to_csv('data/raw/latest_scrape.csv', index=False)
    
    # Create README
    readme = f"""# Georgia House Flip Data

## Latest Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Summary
- Total Flips Identified: {len(flips)}
- Total Investors: {len(investors)}
- Total Profit: ${stats['total_profit']:,.2f}
- Average ROI: {stats['avg_roi']:.1f}%

### Files
- `dashboard_data.json` - Complete data for dashboard
- `flips.csv` - All detected flips
- `investors.csv` - All identified investors

### Criteria
- Hold Period: 30-365 days
- Profit Range: $70,000-$150,000
"""
    
    with open('data/README.md', 'w') as f:
        f.write(readme)
    
    print("="*60)
    print("PIPELINE COMPLETE")
    print(f"Flips Found: {len(flips)}")
    print(f"Investors: {len(investors)}")
    print(f"Total Profit: ${stats['total_profit']:,.2f}")
    print("Files saved to data/ directory")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()
