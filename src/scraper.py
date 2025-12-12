import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
import sqlite3
import random
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GeorgiaPropertyScraper:
    def __init__(self, county_config):
        self.county_config = county_config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.investors = self._load_investors()
        self.properties = []  # Store properties for generating flip chains
    
    def _load_investors(self):
        """Load list of investor names for realistic data generation"""
        return [
            "Atlanta Flip Masters LLC", "Georgia Property Investors", "Peachtree RE Group",
            "Southern Holdings Inc", "Metro Atlanta Investments", "Capital Flip Group",
            "Quick Turn Properties", "Urban Development LLC", "Residential Ventures",
            "Property Solutions GA", "Investor Collective", "Renovation Experts",
            "Fix & Flip GA", "Atlanta Capital Partners", "Georgia Home Solutions",
            "Peach State Investments", "Metro Flip Team", "Georgia Renovation Group"
        ]
    
    def get_recent_sales(self, days_back=180):
        """Main method to get both buy and sell transactions"""
        logger.info(f"Generating data for {self.county_config['name']}")
        
        # Generate buy transactions
        buy_data = self._generate_buy_transactions(days_back)
        
        # Generate sell transactions (flips)
        sell_data = self._generate_sell_transactions(buy_data)
        
        # Combine all transactions
        all_data = pd.concat([buy_data, sell_data], ignore_index=True)
        
        logger.info(f"  Generated {len(buy_data)} buy and {len(sell_data)} sell transactions")
        
        return all_data
    
    def _generate_buy_transactions(self, days_back=180, count=50):
        """Generate buy transactions (initial purchases)"""
        data = []
        county_name = self.county_config['name']
        
        # Generate base buy dates
        base_date = datetime.now() - timedelta(days=days_back)
        
        for i in range(count):
            # Generate property ID
            property_id = f"{county_name[:3].upper()}{i:06d}"
            
            # Generate buy date within the range
            buy_date = base_date + timedelta(days=random.randint(0, days_back-30))
            
            # Generate realistic prices based on county
            buy_price = self._generate_buy_price(county_name)
            
            # Randomly decide if this will be flipped (70% chance for investor purchases)
            will_flip = random.random() < 0.7
            
            # Determine buyer - investor for flips, regular buyer for non-flips
            if will_flip:
                buyer = random.choice(self.investors)
            else:
                buyer = self._generate_regular_buyer()
            
            # Store property info for potential flip generation
            self.properties.append({
                'property_id': property_id,
                'buy_date': buy_date,
                'buy_price': buy_price,
                'will_flip': will_flip,
                'buyer': buyer,
                'address': self._generate_address(i, county_name)
            })
            
            data.append({
                'property_id': property_id,
                'address': self._generate_address(i, county_name),
                'sale_date': buy_date.strftime('%Y-%m-%d'),
                'sale_price': buy_price,
                'buyer': buyer,
                'seller': self._generate_seller(),
                'county': county_name,
                'recording_date': buy_date.strftime('%Y-%m-%d')
            })
        
        return pd.DataFrame(data)
    
    def _generate_sell_transactions(self, buy_df, flip_percentage=0.4):
        """Generate sell transactions for flip properties"""
        data = []
        
        # Get properties marked for flipping
        flip_properties = [p for p in self.properties if p['will_flip']]
        num_flips = int(len(flip_properties) * flip_percentage)
        
        for i, prop in enumerate(flip_properties[:num_flips]):
            # Generate sell date (30-365 days after buy)
            hold_days = random.randint(30, 365)
            sell_date = prop['buy_date'] + timedelta(days=hold_days)
            
            # Skip if sell date is in the future
            if sell_date > datetime.now():
                continue
            
            # Calculate sell price with profit
            min_profit = random.randint(70000, 150000)
            sell_price = prop['buy_price'] + min_profit
            
            # Add some variation
            sell_price = int(sell_price * random.uniform(0.95, 1.15))
            
            # Find the original buyer from buy transactions
            original_buy = buy_df[buy_df['property_id'] == prop['property_id']]
            if not original_buy.empty:
                original_buyer = original_buy.iloc[0]['buyer']
                
                data.append({
                    'property_id': prop['property_id'],
                    'address': prop['address'],
                    'sale_date': sell_date.strftime('%Y-%m-%d'),
                    'sale_price': sell_price,
                    'buyer': self._generate_regular_buyer(),  # New buyer (end user)
                    'seller': original_buyer,  # Original investor is now seller
                    'county': self.county_config['name'],
                    'recording_date': sell_date.strftime('%Y-%m-%d')
                })
        
        return pd.DataFrame(data)
    
    def _generate_buy_price(self, county_name):
        """Generate realistic buy price based on county"""
        if "Fulton" in county_name or "Gwinnett" in county_name or "Cobb" in county_name:
            return random.randint(200000, 500000)
        elif "DeKalb" in county_name or "Clayton" in county_name:
            return random.randint(150000, 350000)
        elif "Chatham" in county_name or "Cherokee" in county_name:
            return random.randint(180000, 400000)
        else:
            return random.randint(80000, 250000)
    
    def _generate_address(self, index, county_name):
        """Generate realistic address based on county"""
        street_numbers = [str(random.randint(100, 9999))]
        street_names = [
            "Main", "Oak", "Maple", "Pine", "Cedar", "Elm", "Washington", "Jefferson",
            "Lincoln", "Broad", "Market", "Peachtree", "Sugarloaf", "Barrett", "Memorial"
        ]
        street_types = ["St", "Ave", "Rd", "Blvd", "Ln", "Dr", "Pkwy", "Way"]
        
        # County-specific city mapping
        cities = {
            "Fulton": ["Atlanta", "Sandy Springs", "Roswell", "Johns Creek"],
            "Gwinnett": ["Lawrenceville", "Duluth", "Norcross", "Snellville"],
            "Cobb": ["Marietta", "Smyrna", "Kennesaw", "Acworth"],
            "DeKalb": ["Decatur", "Dunwoody", "Stone Mountain", "Clarkston"],
            "Clayton": ["Jonesboro", "Forest Park", "Riverdale", "Lovejoy"],
            "Chatham": ["Savannah", "Pooler", "Garden City", "Port Wentworth"],
            "Cherokee": ["Canton", "Woodstock", "Holly Springs", "Ball Ground"]
        }
        
        # Get city based on county
        county_key = county_name.split()[0]  # Get first word
        if county_key in cities:
            city = random.choice(cities[county_key])
        else:
            city = county_key
        
        address = f"{random.choice(street_numbers)} {random.choice(street_names)} {random.choice(street_types)}"
        zip_code = f"300{random.randint(10, 99)}"
        
        return f"{address}, {city}, GA {zip_code}"
    
    def _generate_seller(self):
        """Generate seller names (non-investors)"""
        sellers = [
            "John Smith", "Mary Johnson", "Robert Williams", "Jennifer Brown",
            "Michael Jones", "Linda Garcia", "William Miller", "Elizabeth Davis",
            "David Rodriguez", "Susan Martinez", "Richard Hernandez", "Karen Lopez",
            "Charles Gonzalez", "Nancy Wilson", "Thomas Anderson", "Betty Taylor",
            "Daniel Thomas", "Dorothy Moore", "Matthew Jackson", "Carol Martin",
            "Estate of James Lee", "Trust of Patricia White", "Bank REO", "Relocation LLC"
        ]
        return random.choice(sellers)
    
    def _generate_regular_buyer(self):
        """Generate regular home buyer names (non-investors)"""
        regular_buyers = [
            "James Wilson", "Patricia Moore", "Robert Taylor", "Linda Anderson",
            "Michael Thomas", "Barbara Jackson", "William White", "Elizabeth Harris",
            "David Martin", "Jennifer Thompson", "Richard Garcia", "Maria Martinez",
            "Joseph Robinson", "Susan Clark", "Charles Rodriguez", "Sarah Lewis",
            "Christopher Lee", "Karen Walker", "Daniel Hall", "Lisa Allen"
        ]
        return random.choice(regular_buyers)
