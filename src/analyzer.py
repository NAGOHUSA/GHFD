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
        if df.empty:
            return pd.DataFrame()
        
        flips = []
        
        for i in range(len(df) - 1):
            for j in range(i + 1, len(df)):
                if df.iloc[i]['property_id'] == df.iloc[j]['property_id']:
                    buy = df.iloc[i]
                    sell = df.iloc[j]
                    
                    hold_days = (datetime.strptime(sell['sale_date'], '%Y-%m-%d') - 
                                datetime.strptime(buy['sale_date'], '%Y-%m-%d')).days
                    
                    profit = sell['sale_price'] - buy['sale_price']
                    
                    if (self.config['min_hold_days'] <= hold_days <= self.config['max_hold_days'] and
                        self.config['min_profit'] <= profit <= self.config['max_profit']):
                        
                        flips.append({
                            'property_id': buy['property_id'],
                            'address': buy['address'],
                            'buy_date': buy['sale_date'],
                            'buy_price': buy['sale_price'],
                            'buyer': buy['buyer'],
                            'sell_date': sell['sale_date'],
                            'sell_price': sell['sale_price'],
                            'seller': sell['seller'],
                            'hold_days': hold_days,
                            'profit': profit,
                            'roi': (profit / buy['sale_price']) * 100,
                            'county': buy['county']
                        })
        
        return pd.DataFrame(flips)
    
    def analyze_investors(self, flips_df):
        if flips_df.empty:
            return pd.DataFrame()
        
        investor_stats = flips_df.groupby('buyer').agg({
            'property_id': 'count',
            'profit': ['sum', 'mean'],
            'hold_days': 'mean',
            'roi': 'mean'
        }).reset_index()
        
        investor_stats.columns = [
            'investor_name',
            'total_flips',
            'total_profit',
            'avg_profit_per_flip',
            'avg_hold_days',
            'avg_roi'
        ]
        
        return investor_stats.sort_values('total_flips', ascending=False)
