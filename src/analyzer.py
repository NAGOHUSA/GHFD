import pandas as pd
from datetime import datetime
import sqlite3
import json

class FlipAnalyzer:
    def __init__(self):
        self.config = {
            'min_hold_days': 30,
            'max_hold_days': 365,
            'min_profit': 70000,
            'max_profit': 150000
        }
    
    def identify_flips(self, df):
        """Optimized flip identification using groupby and sorting"""
        if df.empty or 'property_id' not in df.columns:
            return pd.DataFrame()
        
        # Convert date strings to datetime for comparison
        df = df.copy()
        df['sale_date'] = pd.to_datetime(df['sale_date'])
        
        flips = []
        
        # Group by property_id and process each property
        for prop_id, group in df.groupby('property_id'):
            # Sort transactions by date
            group = group.sort_values('sale_date')
            
            # If we have at least 2 transactions for this property
            if len(group) >= 2:
                # Look for buy-sell pairs (investor buying then selling)
                for i in range(len(group) - 1):
                    for j in range(i + 1, len(group)):
                        buy = group.iloc[i]
                        sell = group.iloc[j]
                        
                        # Skip if same buyer/seller (not a flip)
                        if buy['buyer'] == sell['seller']:
                            hold_days = (sell['sale_date'] - buy['sale_date']).days
                            profit = sell['sale_price'] - buy['sale_price']
                            
                            if (self.config['min_hold_days'] <= hold_days <= self.config['max_hold_days'] and
                                self.config['min_profit'] <= profit <= self.config['max_profit']):
                                
                                flips.append({
                                    'property_id': prop_id,
                                    'address': buy['address'],
                                    'buy_date': buy['sale_date'].strftime('%Y-%m-%d'),
                                    'buy_price': float(buy['sale_price']),
                                    'buyer': buy['buyer'],
                                    'sell_date': sell['sale_date'].strftime('%Y-%m-%d'),
                                    'sell_price': float(sell['sale_price']),
                                    'seller': sell['seller'],
                                    'hold_days': int(hold_days),
                                    'profit': float(profit),
                                    'roi': float((profit / buy['sale_price']) * 100),
                                    'county': buy['county']
                                })
        
        return pd.DataFrame(flips)
    
    def analyze_investors(self, flips_df):
        if flips_df.empty:
            return pd.DataFrame()
        
        # Group by investor
        investor_stats = flips_df.groupby('buyer').agg({
            'property_id': 'count',
            'profit': ['sum', 'mean'],
            'hold_days': 'mean'
        }).reset_index()
        
        # Flatten column names
        investor_stats.columns = [
            'investor_name',
            'total_flips',
            'total_profit',
            'avg_profit_per_flip',
            'avg_hold_days'
        ]
        
        # Calculate ROI
        investor_stats['avg_roi'] = (investor_stats['avg_profit_per_flip'] / 
                                   (investor_stats['total_profit'] / investor_stats['total_flips'])) * 100
        
        return investor_stats.sort_values('total_flips', ascending=False)
