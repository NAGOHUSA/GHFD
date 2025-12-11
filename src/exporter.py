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
            investor_flips = flips_df[flips_df['buyer'] == investor_name]
            
            # Create profile
            profile = {
                'investor_name': investor_name,
                'total_flips': investor['total_flips'],
                'total_profit': investor['total_profit'],
                'avg_hold_days': investor['avg_hold_days'],
                'avg_roi': investor['avg_roi'],
                'recent_property_count': len(investor_flips),
                'first_flip_date': investor_flips['buy_date'].min().strftime('%Y-%m-%d') if not investor_flips.empty else '',
                'last_flip_date': investor_flips['sell_date'].max().strftime('%Y-%m-%d') if not investor_flips.empty else '',
                'estimated_yearly_volume': investor['total_profit'] * 12 / investor['avg_hold_days'] if investor['avg_hold_days'] > 0 else 0,
                'likely_loan_needs': 'HIGH' if investor['total_flips'] > 5 else 'MEDIUM'
            }
            
            # Try to extract contact info (simplified - would need actual lookup)
            profile.update(self._extract_contact_info(investor_name, investor_flips))
            
            enriched_investors.append(profile)
        
        # Convert to DataFrame
        contact_df = pd.DataFrame(enriched_investors)
        
        # Sort by priority (total flips + volume)
        contact_df['priority_score'] = (
            contact_df['total_flips'] * 0.4 +
            contact_df['total_profit'] * 0.3 +
            contact_df['estimated_yearly_volume'] * 0.3
        )
        contact_df = contact_df.sort_values('priority_score', ascending=False)
        
        # Save output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data/results/investor_contacts_{timestamp}'
        
        if output_format == 'csv':
            contact_df.to_csv(f'{filename}.csv', index=False)
        elif output_format == 'excel':
            contact_df.to_excel(f'{filename}.xlsx', index=False)
        elif output_format == 'json':
            contact_df.to_json(f'{filename}.json', orient='records', indent=2)
        
        logger.info(f"Generated contact list with {len(contact_df)} investors")
        return filename
    
    def _extract_contact_info(self, investor_name: str, flips_df: pd.DataFrame) -> Dict:
        """Extract potential contact information from flip data"""
        # This is a simplified version
        # In production, you might:
        # 1. Search business registrations
        # 2. Look up addresses from deeds
        # 3. Use commercial data APIs
        
        contact_info = {
            'business_type': self._infer_business_type(investor_name),
            'likely_location': self._infer_location(flips_df) if not flips_df.empty else '',
            'flip_counties': ', '.join(flips_df['county'].unique()) if not flips_df.empty else '',
            'property_types': 'Residential'  # Simplified
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
        if flips_df.empty:
            return ''
        
        # Get most common city from addresses
        addresses = flips_df['address'].dropna().tolist()
        cities = []
        
        for addr in addresses:
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
    
    def generate_outreach_template(self, investor_profile: Dict) -> str:
        """Generate email template for outreach"""
        template = f"""
Subject: Quick Bridge Loan Opportunity for Georgia Flippers

Hi {investor_profile['investor_name']},

I noticed your recent successful property flips in {investor_profile['flip_counties']} County. 
With {investor_profile['total_flips']} completed flips, you're clearly an active investor.

We specialize in fast bridge loans (7-30 day closings) for Georgia house flippers:
• Rates starting at 9.99%
• Up to 75% LTV
• Close in as little as 7 days
• No income verification needed

Would you be open to a 15-minute chat next week to discuss your upcoming projects?

Best regards,
[Your Name]
[Your Company]
[Phone Number]
        """
        
        return template.strip()

if __name__ == "__main__":
    # Example usage
    exporter = InvestorExporter()
    
    # Load sample data
    import pandas as pd
    flips = pd.read_csv('data/results/flips_detailed_20240101_120000.csv')
    investors = pd.read_csv('data/results/investors_20240101_120000.csv')
    
    # Generate contact list
    output_file = exporter.generate_contact_list(investors, flips, 'excel')
    print(f"Generated: {output_file}.xlsx")
