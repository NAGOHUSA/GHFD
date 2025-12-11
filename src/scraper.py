import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
import sqlite3
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GeorgiaPropertyScraper:
    """Scrape property data from Georgia county qPublic websites"""
    
    def __init__(self, county_config: Dict):
        self.county_config = county_config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_recent_sales(self, days_back: int = 180) -> pd.DataFrame:
        """Get recent sales data from the county website"""
        logger.info(f"Fetching recent sales for {self.county_config['name']}")
        
        # This is a simplified example - actual implementation would need
        # to handle each county's specific API/website structure
        try:
            # For demonstration, we'll create mock data
            # In production, you'd implement actual scraping logic here
            mock_data = self._create_mock_data()
            return mock_data
            
        except Exception as e:
            logger.error(f"Error scraping {self.county_config['name']}: {e}")
            return pd.DataFrame()
    
    def _create_mock_data(self) -> pd.DataFrame:
        """Create mock data for demonstration"""
        # In a real implementation, this would be actual scraping code
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        
        data = []
        for i in range(100):
            sale_date = dates[i]
            price = 200000 + i * 10000
            data.append({
                'property_id': f"PROP{1000 + i}",
                'address': f"{100 + i} Mock St, Atlanta, GA 3030{i % 10}",
                'sale_date': sale_date,
                'sale_price': price,
                'buyer': f"Buyer {i}",
                'seller': f"Seller {i}",
                'county': self.county_config['name'],
                'recording_date': sale_date
            })
        
        return pd.DataFrame(data)
    
    def save_to_database(self, df: pd.DataFrame, db_path: str = 'data/properties.db'):
        """Save scraped data to SQLite database"""
        conn = sqlite3.connect(db_path)
        
        # Create table if it doesn't exist
        conn.execute('''
            CREATE TABLE IF NOT EXISTS property_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT,
                address TEXT,
                sale_date DATE,
                sale_price REAL,
                buyer TEXT,
                seller TEXT,
                county TEXT,
                recording_date DATE,
                scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index for faster queries
        conn.execute('CREATE INDEX IF NOT EXISTS idx_property_id ON property_sales(property_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sale_date ON property_sales(sale_date)')
        
        # Save to database
        df.to_sql('property_sales', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {len(df)} records to database")

class MultiCountyScraper:
    """Scrape data from multiple Georgia counties"""
    
    def __init__(self, config_path: str = 'config/counties.json'):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.scrapers = {}
        for county_id, county_config in self.config['counties'].items():
            if county_config.get('enabled', True):
                self.scrapers[county_id] = GeorgiaPropertyScraper(county_config)
    
    def scrape_all_counties(self) -> pd.DataFrame:
        """Scrape data from all enabled counties"""
        all_data = []
        
        for county_id, scraper in self.scrapers.items():
            logger.info(f"Scraping {county_id}...")
            data = scraper.get_recent_sales()
            if not data.empty:
                all_data.append(data)
            
            # Be respectful with rate limiting
            time.sleep(2)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

if __name__ == "__main__":
    scraper = MultiCountyScraper()
    data = scraper.scrape_all_counties()
    if not data.empty:
        scraper.scrapers['fulton'].save_to_database(data)
        data.to_csv('data/raw/recent_sales.csv', index=False)
        print(f"Scraped {len(data)} records")
