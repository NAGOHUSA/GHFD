#!/usr/bin/env python3
"""
Historical Data Archiver for Georgia House Flip Pipeline
Saves daily snapshots and maintains a historical index.
"""

import json
import os
import shutil
from datetime import datetime, timedelta
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalDataArchiver:
    def __init__(self, config_path='../pipeline.json'):
        self.config = self.load_config(config_path)
        self.historical_dir = 'data/historical'
        self.dashboard_dir = 'data/dashboard'
        self.index_file = os.path.join(self.historical_dir, 'historical_index.json')
        
        # Create directories if they don't exist
        os.makedirs(self.historical_dir, exist_ok=True)
        os.makedirs(self.dashboard_dir, exist_ok=True)
    
    def load_config(self, config_path):
        """Load pipeline configuration"""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def archive_today_data(self):
        """Archive today's data and update historical index"""
        today = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Archiving data for {today}")
        
        # Source files to archive
        source_files = {
            'dashboard_data': os.path.join(self.dashboard_dir, 'dashboard_data.json'),
            'flips': 'data/results/flips_latest.csv',
            'investors': 'data/results/investors_latest.csv',
            'summary': 'data/results/pipeline_summary_latest.json'
        }
        
        archived = []
        
        for file_type, source_path in source_files.items():
            if os.path.exists(source_path):
                # Create dated filename
                if file_type == 'dashboard_data':
                    dest_filename = f'dashboard_data_{today}.json'
                elif file_type == 'summary':
                    dest_filename = f'pipeline_summary_{today}.json'
                else:
                    dest_filename = f'{file_type}_{today}.csv'
                
                dest_path = os.path.join(self.historical_dir, dest_filename)
                
                try:
                    # Copy the file
                    shutil.copy2(source_path, dest_path)
                    archived.append(dest_filename)
                    logger.info(f"  Archived {file_type} to {dest_filename}")
                    
                    # Also keep a 'latest' copy for quick access
                    latest_path = os.path.join(self.historical_dir, f'{file_type}_latest.json' 
                                              if file_type in ['dashboard_data', 'summary'] 
                                              else f'{file_type}_latest.csv')
                    shutil.copy2(source_path, latest_path)
                    
                except Exception as e:
                    logger.error(f"  Failed to archive {file_type}: {e}")
            else:
                logger.warning(f"  Source file not found: {source_path}")
        
        # Update historical index
        self.update_historical_index(today, archived)
        
        # Clean up old data
        self.cleanup_old_data()
        
        return archived
    
    def update_historical_index(self, date, archived_files):
        """Update the historical index JSON file"""
        try:
            # Load existing index or create new
            if os.path.exists(self.index_file):
                with open(self.index_file, 'r') as f:
                    index = json.load(f)
            else:
                index = {
                    'available_dates': [],
                    'files_by_date': {},
                    'last_updated': None,
                    'total_archives': 0
                }
            
            # Add new date if not already present
            if date not in index['available_dates']:
                index['available_dates'].append(date)
                index['available_dates'].sort(reverse=True)  # Most recent first
            
            # Update files for this date
            index['files_by_date'][date] = archived_files
            
            # Update metadata
            index['last_updated'] = datetime.now().isoformat()
            index['total_archives'] = len(index['available_dates'])
            
            # Save index
            with open(self.index_file, 'w') as f:
                json.dump(index, f, indent=2)
            
            logger.info(f"Updated historical index with {len(archived_files)} files for {date}")
            
            return index
            
        except Exception as e:
            logger.error(f"Failed to update historical index: {e}")
            return None
    
    def cleanup_old_data(self):
        """Remove historical data older than retention period"""
        retention_days = self.config.get('data_retention_days', 90)
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'r') as f:
                    index = json.load(f)
                
                files_to_delete = []
                dates_to_remove = []
                
                # Check each date in the index
                for date_str in index['available_dates'][:]:  # Copy of list for iteration
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    if date < cutoff_date:
                        # Mark files for deletion
                        if date_str in index['files_by_date']:
                            files_to_delete.extend(index['files_by_date'][date_str])
                        
                        # Mark date for removal from index
                        dates_to_remove.append(date_str)
                
                # Delete old files
                deleted_count = 0
                for filename in files_to_delete:
                    filepath = os.path.join(self.historical_dir, filename)
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        deleted_count += 1
                
                # Update index
                for date_str in dates_to_remove:
                    index['available_dates'].remove(date_str)
                    if date_str in index['files_by_date']:
                        del index['files_by_date'][date_str]
                
                # Save updated index
                with open(self.index_file, 'w') as f:
                    json.dump(index, f, indent=2)
                
                logger.info(f"Cleaned up {deleted_count} files older than {retention_days} days")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
    
    def generate_dashboard_data(self, flips_df, investors_df, summary_stats):
        """Generate dashboard-ready JSON data"""
        dashboard_data = {
            'summary': {
                'total_flips': len(flips_df) if not flips_df.empty else 0,
                'total_investors': len(investors_df) if not investors_df.empty else 0,
                'total_profit': flips_df['profit'].sum() if not flips_df.empty else 0,
                'avg_roi': flips_df['roi'].mean() if not flips_df.empty else 0,
                'timestamp': datetime.now().isoformat(),
                'date': datetime.now().strftime('%Y-%m-%d')
            },
            'recent_flips': [],
            'top_investors': []
        }
        
        # Add recent flips (top 100 by profit)
        if not flips_df.empty:
            top_flips = flips_df.nlargest(100, 'profit')
            for _, flip in top_flips.iterrows():
                dashboard_data['recent_flips'].append({
                    'property_id': flip.get('property_id', ''),
                    'address': flip.get('address', ''),
                    'buy_date': flip.get('buy_date', ''),
                    'buy_price': float(flip.get('buy_price', 0)),
                    'buyer': flip.get('buyer', ''),
                    'sell_date': flip.get('sell_date', ''),
                    'sell_price': float(flip.get('sell_price', 0)),
                    'hold_days': int(flip.get('hold_days', 0)),
                    'profit': float(flip.get('profit', 0)),
                    'roi': float(flip.get('roi', 0)),
                    'county': flip.get('county', '')
                })
        
        # Add top investors (top 50 by total profit)
        if not investors_df.empty:
            top_investors = investors_df.nlargest(50, 'total_profit')
            for _, investor in top_investors.iterrows():
                dashboard_data['top_investors'].append({
                    'investor_name': investor.get('investor_name', ''),
                    'total_flips': int(investor.get('total_flips', 0)),
                    'total_profit': float(investor.get('total_profit', 0)),
                    'avg_profit_per_flip': float(investor.get('avg_profit_per_flip', 0)),
                    'avg_hold_days': float(investor.get('avg_hold_days', 0)),
                    'avg_roi': float(investor.get('avg_roi', 0))
                })
        
        # Add stats by county
        if not flips_df.empty and 'county' in flips_df.columns:
            county_stats = flips_df.groupby('county').agg({
                'property_id': 'count',
                'profit': 'sum',
                'roi': 'mean'
            }).reset_index()
            
            by_county = {}
            for _, row in county_stats.iterrows():
                by_county[row['county']] = {
                    'flips': int(row['property_id']),
                    'total_profit': float(row['profit']),
                    'avg_roi': float(row['roi'])
                }
            
            dashboard_data['stats_by_county'] = by_county
        
        return dashboard_data
    
    def run(self, flips_df=None, investors_df=None, summary_stats=None):
        """Main entry point: generate dashboard data and archive it"""
        logger.info("Starting historical data archiving process")
        
        # Generate dashboard data
        if flips_df is not None and investors_df is not None:
            dashboard_data = self.generate_dashboard_data(flips_df, investors_df, summary_stats)
            
            # Save dashboard data
            dashboard_file = os.path.join(self.dashboard_dir, 'dashboard_data.json')
            with open(dashboard_file, 'w') as f:
                json.dump(dashboard_data, f, indent=2)
            
            logger.info(f"Generated dashboard data with {len(dashboard_data['recent_flips'])} flips")
        
        # Archive today's data
        archived = self.archive_today_data()
        
        logger.info(f"Historical archiving complete. Archived {len(archived)} files.")
        return archived

if __name__ == "__main__":
    # Example usage
    archiver = HistoricalDataArchiver()
    
    # Load example data (in production, this would come from the pipeline)
    try:
        flips_df = pd.read_csv('data/results/flips_latest.csv')
        investors_df = pd.read_csv('data/results/investors_latest.csv')
        summary_stats = {
            'total_transactions': 1000,
            'unique_counties': 50,
            'date_range': '2023-01-01 to 2023-12-31'
        }
        
        archiver.run(flips_df, investors_df, summary_stats)
    except Exception as e:
        logger.error(f"Failed to run archiver: {e}")
        # Even if data files don't exist, still run to cleanup old data
        archiver.run()
