import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from typing import List, Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlipAnalyzer:
    """Analyze property sales to identify potential flips"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {
            'min_hold_days': 30,
            'max_hold_days': 365,
            'min_profit': 70000,
            'max_profit': 150000
        }
    
    def load_from_database(self, db_path: str = 'data/properties.db') -> pd.DataFrame:
        """Load property sales from SQLite database"""
        conn = sqlite3.connect(db_path)
        query = """
        SELECT * FROM property_sales 
        WHERE sale_date >= date('now', '-2 years')
        ORDER BY property_id, sale_date
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            logger.warning("No data found in database")
            return df
        
        # Convert date columns
        date_columns = ['sale_date', 'recording_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        return df
    
    def identify_flips(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify properties that were flipped based on criteria"""
        
        if df.empty:
            return pd.DataFrame()
        
        # Group by property and find consecutive sales
        flips = []
        
        for property_id, group in df.groupby('property_id'):
            # Sort by sale date
            group = group.sort_values('sale_date')
            
            # Need at least 2 transactions
            if len(group) < 2:
                continue
            
            # Check each consecutive pair of transactions
            for i in range(len(group) - 1):
                buy_transaction = group.iloc[i]
                sell_transaction = group.iloc[i + 1]
                
                # Calculate hold period
                hold_days = (sell_transaction['sale_date'] - buy_transaction['sale_date']).days
                
                # Calculate profit
                profit = sell_transaction['sale_price'] - buy_transaction['sale_price']
                
                # Check if it meets flip criteria
                if (self.config['min_hold_days'] <= hold_days <= self.config['max_hold_days'] and
                    self.config['min_profit'] <= profit <= self.config['max_profit']):
                    
                    flips.append({
                        'property_id': property_id,
                        'address': buy_transaction['address'],
                        'buy_date': buy_transaction['sale_date'],
                        'buy_price': buy_transaction['sale_price'],
                        'buyer': buy_transaction['buyer'],
                        'sell_date': sell_transaction['sale_date'],
                        'sell_price': sell_transaction['sale_price'],
                        'seller': sell_transaction['seller'],
                        'hold_days': hold_days,
                        'profit': profit,
                        'roi': (profit / buy_transaction['sale_price']) * 100,
                        'county': buy_transaction['county']
                    })
        
        return pd.DataFrame(flips)
    
    def analyze_investors(self, flips_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate flip data by investor"""
        if flips_df.empty:
            return pd.DataFrame()
        
        investor_stats = flips_df.groupby('buyer').agg({
            'property_id': 'count',
            'profit': ['sum', 'mean'],
            'hold_days': 'mean',
            'roi': 'mean'
        }).reset_index()
        
        # Flatten column names
        investor_stats.columns = [
            'investor_name',
            'total_flips',
            'total_profit',
            'avg_profit_per_flip',
            'avg_hold_days',
            'avg_roi'
        ]
        
        return investor_stats.sort_values('total_flips', ascending=False)
    
    def generate_report(self, flips_df: pd.DataFrame, 
                       investors_df: pd.DataFrame,
                       output_dir: str = 'data/results') -> Dict:
        """Generate comprehensive analysis report"""
        
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        report = {
            'total_flips_identified': len(flips_df),
            'total_investors': len(investors_df),
            'total_profit': flips_df['profit'].sum() if not flips_df.empty else 0,
            'avg_hold_days': flips_df['hold_days'].mean() if not flips_df.empty else 0,
            'avg_roi': flips_df['roi'].mean() if not flips_df.empty else 0,
            'by_county': flips_df.groupby('county').size().to_dict() if not flips_df.empty else {}
        }
        
        # Save detailed data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if not flips_df.empty:
            flips_df.to_csv(f'{output_dir}/flips_detailed_{timestamp}.csv', index=False)
            flips_df.to_excel(f'{output_dir}/flips_detailed_{timestamp}.xlsx', index=False)
        
        if not investors_df.empty:
            investors_df.to_csv(f'{output_dir}/investors_{timestamp}.csv', index=False)
            investors_df.to_excel(f'{output_dir}/investors_{timestamp}.xlsx', index=False)
        
        # Save report as JSON
        import json
        with open(f'{output_dir}/report_{timestamp}.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        return report

if __name__ == "__main__":
    analyzer = FlipAnalyzer()
    data = analyzer.load_from_database()
    
    if not data.empty:
        flips = analyzer.identify_flips(data)
        investors = analyzer.analyze_investors(flips)
        report = analyzer.generate_report(flips, investors)
        
        print(f"Identified {len(flips)} potential flips")
        print(f"Found {len(investors)} active investors")
        
        if not investors.empty:
            print("\nTop 10 Investors:")
            print(investors.head(10).to_string())
