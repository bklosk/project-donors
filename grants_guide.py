"""
Grants Data Collection Guide and Alternative Approaches
"""

import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class GrantsDataCollector:
    """Comprehensive grants data collection using multiple approaches"""
    
    def __init__(self):
        self.approaches = {
            'propublica': 'ProPublica API (limited)',
            'irs_990': 'IRS 990 XML parsing (complex)',
            'candid': 'Candid/Foundation Directory (paid)',
            'guidestar': 'GuideStar (limited free tier)',
            'manual': 'Manual research approach'
        }
    
    def explain_grants_data_sources(self) -> None:
        """Explain different approaches to getting grants data"""
        print("\n" + "="*60)
        print("📊 COMPREHENSIVE GRANTS DATA COLLECTION GUIDE")
        print("="*60)
        
        print("\n🎯 BEST APPROACHES FOR GRANTS DATA:")
        print("-" * 40)
        
        print("\n1. 🏆 CANDID (Foundation Directory Online)")
        print("   • Most comprehensive grants database")
        print("   • Covers 140,000+ foundations")
        print("   • Detailed grant recipient information")
        print("   • Cost: $179+/month subscription")
        print("   • URL: https://foundationdirectory.org/")
        
        print("\n2. 📋 IRS 990 FORMS (Free but complex)")
        print("   • Schedule I contains detailed grants data")
        print("   • Available for foundations and large nonprofits")
        print("   • Requires parsing PDF or XML files")
        print("   • 1-2 year delay in availability")
        print("   • Sources:")
        print("     - ProPublica Nonprofit Explorer (PDF)")
        print("     - IRS AWS S3 buckets (XML)")
        print("     - GuideStar (requires registration)")
        
        print("\n3. 🔍 GUIDESTAR/CANDID ESSENTIALS (Limited free)")
        print("   • Basic grants information")
        print("   • Summary data only")
        print("   • Registration required")
        print("   • URL: https://www.guidestar.org/")
        
        print("\n4. 🏛️ FOUNDATION WEBSITES")
        print("   • Many foundations publish grants lists")
        print("   • Most current information")
        print("   • Varying formats and completeness")
        print("   • Requires manual research")
        
        print("\n5. 📰 GRANT DATABASES & NEWS")
        print("   • Chronicle of Philanthropy")
        print("   • Foundation News & Commentary")
        print("   • Local business journals")
        print("   • Press releases")
        
        print("\n" + "="*60)
        print("💡 RECOMMENDED APPROACH FOR YOUR PROJECT:")
        print("="*60)
        
        print("\n🥇 IMMEDIATE (Free): Use ProPublica + Manual Research")
        print("   1. Use our script to get foundation contact info")
        print("   2. Visit foundation websites for published grants")
        print("   3. Search news articles for major grants")
        print("   4. Check GuideStar for basic information")
        
        print("\n🥈 THOROUGH (Some cost): IRS 990 Analysis")
        print("   1. Download 990 forms from ProPublica")
        print("   2. Focus on Schedule I for grants data")
        print("   3. Use PDF parsing tools or manual entry")
        print("   4. Build database over time")
        
        print("\n🥉 COMPREHENSIVE (Paid): Professional Database")
        print("   1. Subscribe to Candid Foundation Directory")
        print("   2. Export grants data for your target foundations")
        print("   3. Combine with our organizational data")
        print("   4. Most complete and current information")
        
        print("\n" + "="*60)
    
    def create_grants_collection_template(self) -> pd.DataFrame:
        """Create a template for manual grants data collection"""
        
        template_data = {
            'grantor_ein': ['Example: 36-3330462'],
            'grantor_name': ['Example: Crown Family Foundation'],
            'grantor_type': ['Private Foundation'],
            'recipient_name': ['Example: Chicago Public Schools'],
            'recipient_ein': ['Example: 36-4567890'],
            'recipient_city': ['Chicago'],
            'recipient_state': ['IL'],
            'grant_amount': [1000000],
            'grant_purpose': ['Educational programming and teacher training'],
            'grant_year': [2023],
            'data_source': ['Foundation website'],
            'source_url': ['https://example.org/grants'],
            'collection_date': ['2025-06-26'],
            'notes': ['Multi-year commitment, first of three payments']
        }
        
        df = pd.DataFrame(template_data)
        return df
    
    def save_grants_template(self) -> str:
        """Save a grants collection template"""
        template = self.create_grants_collection_template()
        filename = 'data/grants_collection_template.csv'
        template.to_csv(filename, index=False)
        
        print(f"\n📋 Grants collection template saved: {filename}")
        print("   • Use this template to manually collect grants data")
        print("   • Add rows for each grant you find")
        print("   • Import back into your analysis")
        
        return filename
    
    def generate_foundation_research_list(self, organizations: List[Dict]) -> pd.DataFrame:
        """Generate a prioritized list for manual grants research"""
        
        # Filter for larger, established foundations
        research_targets = []
        
        for org in organizations:
            # Prioritize based on certain criteria
            priority_score = 0
            
            # Higher priority for private foundations
            if org.get('organization_type') == 'Private Foundation':
                priority_score += 2
            
            # Higher priority for family foundations (often more transparent)
            if 'family' in org.get('name', '').lower():
                priority_score += 1
            
            # Higher priority for larger/established foundations
            if org.get('score', 0) > 50:  # Higher search relevance score
                priority_score += 1
            
            # Higher priority for those with websites/filings
            if org.get('have_filings'):
                priority_score += 1
            
            research_targets.append({
                'ein': org.get('strein') or org.get('ein'),
                'name': org.get('name'),
                'city': org.get('city'),
                'state': org.get('state'),
                'organization_type': org.get('organization_type'),
                'priority_score': priority_score,
                'research_url': f"https://projects.propublica.org/nonprofits/organizations/{org.get('strein') or org.get('ein')}",
                'website_search': f"site:{org.get('name', '').replace(' ', '').lower()}.org grants",
                'news_search': f'"{org.get('name')}" grants donations',
                'status': 'Not Started',
                'notes': ''
            })
        
        # Sort by priority
        research_df = pd.DataFrame(research_targets)
        research_df = research_df.sort_values('priority_score', ascending=False)
        
        return research_df
    
    def save_research_plan(self, organizations: List[Dict]) -> str:
        """Save a complete grants research plan"""
        
        # Generate research targets
        research_df = self.generate_foundation_research_list(organizations)
        
        # Save to CSV
        filename = 'data/grants_research_plan.csv'
        research_df.to_csv(filename, index=False)
        
        print(f"\n🎯 Grants research plan saved: {filename}")
        print(f"   • {len(research_df)} organizations prioritized for research")
        print(f"   • Top priority: {research_df.iloc[0]['name']}")
        print("   • Includes research URLs and search strategies")
        print("   • Update 'status' and 'notes' columns as you research")
        
        return filename

def main():
    """Demonstrate grants data collection approaches"""
    collector = GrantsDataCollector()
    
    # Show comprehensive guide
    collector.explain_grants_data_sources()
    
    # Create templates
    collector.save_grants_template()
    
    print("\n✅ Grants data collection resources created!")
    print("📁 Check the /data folder for templates and guides")

if __name__ == "__main__":
    main()
