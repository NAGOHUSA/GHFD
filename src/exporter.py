import pandas as pd
from datetime import datetime
import json
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InvestorExporter:
    """Export investor data for outreach"""
    
    def __init__(self):
        self.template = {
            'name': '',
            'company': '',
            'total_flips': 0,
            'total_volume': 0,
            'avg_hold_days': 0,
            'avg_profit': 0,
            'recent_flips': [],
            'contact_info': {}
        }
    
    def generate_contact_list(self, investors_df: pd.DataFrame, 
                            flips_df: pd.DataFrame,
                            output_format: str = 'csv') -> str:
        """Generate contact list for investor outreach"""
        
        # Create enriched investor profiles
        enriched_investors = []
        
        for _, investor in investors_df.iterrows():
            investor_name = investor['investor_name']
            
            # Get this investor's recent flips
            investor_flips = flips_df[flips_df['buyer'] == investor_name].copy()
            
            # Convert date columns to datetime if they're not already
            if not investor_flips.empty and 'buy_date' in investor_flips.columns:
                # If dates are strings, convert to datetime
                if isinstance(investor_flips['buy_date'].iloc[0], str):
                    investor_flips['buy_date'] = pd.to_datetime(investor_flips['buy_date'])
                    investor_flips['sell_date'] = pd.to_datetime(investor_flips['sell_date'])
            
            # Create profile
            profile = {
                'investor_name': investor_name,
                'total_flips': int(investor['total_flips']),
                'total_profit': float(investor['total_profit']),
                'avg_hold_days': float(investor['avg_hold_days']),
                'avg_roi': float(investor['avg_roi']),
                'recent_property_count': len(investor_flips),
                'estimated_yearly_volume': float(investor['total_profit'] * 12 / investor['avg_hold_days']) if investor['avg_hold_days'] > 0 else 0,
                'likely_loan_needs': 'HIGH' if investor['total_flips'] > 5 else 'MEDIUM'
            }
            
            # Add date fields if available
            if not investor_flips.empty and 'buy_date' in investor_flips.columns:
                profile['first_flip_date'] = investor_flips['buy_date'].min().strftime('%Y-%m-%d')
                profile['last_flip_date'] = investor_flips['sell_date'].max().strftime('%Y-%m-%d')
            else:
                profile['first_flip_date'] = ''
                profile['last_flip_date'] = ''
            
            # Try to extract contact info
            profile.update(self._extract_contact_info(investor_name, investor_flips))
            
            enriched_investors.append(profile)
        
        # Convert to DataFrame
        contact_df = pd.DataFrame(enriched_investors)
        
        # Sort by priority (total flips + volume)
        if not contact_df.empty:
            contact_df['priority_score'] = (
                contact_df['total_flips'] * 0.4 +
                contact_df['total_profit'] * 0.3 +
                contact_df['estimated_yearly_volume'] * 0.3
            )
            contact_df = contact_df.sort_values('priority_score', ascending=False)
        
        # Save output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'../data/results/investor_contacts_{timestamp}'
        
        if output_format == 'csv':
            contact_df.to_csv(f'{filename}.csv', index=False)
        elif output_format == 'excel':
            contact_df.to_excel(f'{filename}.xlsx', index=False)
        elif output_format == 'json':
            contact_df.to_json(f'{filename}.json', orient='records', indent=2)
        
        logger.info(f"Generated contact list with {len(contact_df)} investors")
        return f'{filename}.csv'
    
    def _extract_contact_info(self, investor_name: str, flips_df: pd.DataFrame) -> Dict:
        """Extract potential contact information from flip data"""
        contact_info = {
            'business_type': self._infer_business_type(investor_name),
            'likely_location': self._infer_location(flips_df) if not flips_df.empty else '',
            'flip_counties': ', '.join(flips_df['county'].unique().tolist()) if not flips_df.empty else '',
            'property_types': 'Residential'
        }
        
        return contact_info
    
    def _infer_business_type(self, name: str) -> str:
        """Infer business type from name"""
        name_lower = name.lower()
        
        if any(x in name_lower for x in ['llc', 'inc', 'corp', 'ltd']):
            return 'Business Entity'
        elif any(x in name_lower for x in ['trust', 'estate']):
            return 'Trust/Entity'
        else:
            return 'Individual Investor'
    
    def _infer_location(self, flips_df: pd.DataFrame) -> str:
        """Infer likely location from property addresses"""
        if flips_df.empty or 'address' not in flips_df.columns:
            return ''
        
        # Get most common city from addresses
        addresses = flips_df['address'].dropna().tolist()
        cities = []
        
        for addr in addresses:
            if isinstance(addr, str):
                parts = addr.split(',')
                if len(parts) >= 2:
                    city = parts[-2].strip()
                    cities.append(city)
        
        if cities:
            from collections import Counter
            most_common = Counter(cities).most_common(1)
            if most_common:
                return most_common[0][0]
        
        return ''
