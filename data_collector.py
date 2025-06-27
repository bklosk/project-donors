"""
Comprehensive Nonprofit Data Collector
Consolidated script for collecting donor-advised funds, private foundations, and grants data
Includes IRS 990 parsing, grants research tools, and data analysis utilities
"""

import os
import time
import json
import xml.etree.ElementTree as ET
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IRS990Parser:
    """Parser for IRS 990 XML files to extract grants data"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Nonprofit Research Tool 1.0'
        })
        
    def get_990_data_from_propublica(self, ein: str) -> List[Dict]:
        """Get 990 data from ProPublica API"""
        available_files = []
        
        try:
            clean_ein = str(ein).replace('-', '')
            url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{clean_ein}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                filings = data.get('filings_with_data', [])
                logger.info(f"Found {len(filings)} filings for EIN {ein}")
                
                for filing in filings:
                    if filing.get('pdf_url') and filing.get('tax_prd_yr'):
                        available_files.append({
                            'ein': ein,
                            'tax_year': filing['tax_prd_yr'],
                            'pdf_url': filing['pdf_url'],
                            'form_type': filing.get('formtype', 'Unknown')
                        })
                        
        except Exception as e:
            logger.error(f"Error getting 990 data for EIN {ein}: {e}")
            
        return available_files
    
    def _extract_grants_from_xml(self, root: ET.Element, ein: str) -> List[Dict]:
        """Extract grants from XML root element"""
        grants = []
        
        # Look for Schedule I (grants) in various possible locations
        schedule_i_paths = [
            ".//ScheduleI", ".//IRS990ScheduleI", ".//SupplementaryInformationGrp/ScheduleIGrp",
            ".//GrantsAndOtherAssistanceDomesticGrp", ".//GrantsAndOtherAssistanceForeignGrp"
        ]
        
        for path in schedule_i_paths:
            for schedule in root.findall(path):
                grant_entries = (schedule.findall(".//RecipientTable") or 
                               schedule.findall(".//GrantsAndOtherAssistanceToIndividualsInUSGrp"))
                
                for entry in grant_entries:
                    grant_data = self._parse_grant_entry(entry, ein)
                    if grant_data:
                        grants.append(grant_data)
        
        return grants
    
    def _parse_grant_entry(self, entry: ET.Element, grantor_ein: str) -> Optional[Dict]:
        """Parse individual grant entry from XML"""
        try:
            field_mappings = {
                'recipient_name': ['RecipientBusinessName/BusinessNameLine1Txt', 'RecipientPersonNm'],
                'recipient_ein': ['RecipientEIN', 'EINOfRecipient'],
                'amount': ['CashGrantAmt', 'AmountOfCashGrant', 'TotalAmt'],
                'purpose': ['PurposeOfGrantTxt', 'PurposeOfGrant']
            }
            
            grant = {'grantor_ein': grantor_ein}
            
            for field, paths in field_mappings.items():
                value = None
                for path in paths:
                    element = entry.find(f".//{path}")
                    if element is not None and element.text:
                        value = element.text
                        break
                
                if field == 'amount' and value:
                    try:
                        grant[field] = float(value)
                    except (ValueError, TypeError):
                        grant[field] = 0
                else:
                    grant[field] = value or ''
            
            # Only return if we have essential data
            if grant.get('recipient_name') and grant.get('amount', 0) > 0:
                return grant
                
        except Exception as e:
            logger.error(f"Error parsing grant entry: {e}")
            
        return None


class GrantsResearchGuide:
    """Comprehensive grants research guide and resource generator"""
    
    def generate_research_plan(self, foundations_df: pd.DataFrame) -> pd.DataFrame:
        """Generate prioritized research plan for grants data collection"""
        if foundations_df.empty:
            return pd.DataFrame()
        
        research_plan = foundations_df.copy()
        
        # Add research resources
        research_plan['research_priority'] = 'Medium'
        research_plan['candid_url'] = research_plan['name'].apply(
            lambda x: f"https://foundationdirectory.org/search?q={x.replace(' ', '+')}"
        )
        research_plan['guidestar_url'] = research_plan['ein'].apply(
            lambda x: f"https://www.guidestar.org/profile/{x}"
        )
        research_plan['propublica_url'] = research_plan['ein'].apply(
            lambda x: f"https://projects.propublica.org/nonprofits/organizations/{x}"
        )
        
        # Research strategy and tracking
        research_plan['research_strategy'] = ("1. Check organization website for grants list\n"
                                            "2. Search news for recent grant announcements\n"
                                            "3. Review IRS 990 Schedule I if available\n"
                                            "4. Check Candid/GuideStar profiles")
        research_plan['status'] = 'Not Started'
        research_plan['notes'] = ''
        research_plan['grants_found'] = 0
        research_plan['last_updated'] = ''
        
        return research_plan.sort_values(['state', 'name'])[
            ['ein', 'name', 'city', 'state', 'research_priority', 
             'candid_url', 'guidestar_url', 'propublica_url', 
             'research_strategy', 'status', 'notes', 'grants_found', 'last_updated']
        ]
    
    def generate_collection_template(self) -> pd.DataFrame:
        """Generate template for manual grants data collection"""
        return pd.DataFrame({
            'grantor_ein': ['Example: 12-3456789'],
            'grantor_name': ['Example Foundation'],
            'recipient_name': ['Example Nonprofit'],
            'recipient_ein': ['98-7654321'],
            'grant_amount': [50000],
            'grant_purpose': ['General operating support'],
            'grant_year': [2024],
            'data_source': ['Foundation website'],
            'source_url': ['https://example.org/grants'],
            'collection_date': [datetime.now().strftime('%Y-%m-%d')],
            'notes': ['Additional context or details']
        })
    
    def print_comprehensive_guide(self):
        """Print comprehensive grants research guide"""
        guide_text = """
================================================================================
📊 COMPREHENSIVE GRANTS DATA COLLECTION GUIDE
================================================================================

🎯 BEST APPROACHES FOR GRANTS DATA:
--------------------------------------------------

1. 🏆 CANDID (Foundation Directory Online)
   • Most comprehensive grants database
   • Covers 140,000+ foundations
   • Cost: $179+/month subscription
   • URL: https://foundationdirectory.org/

2. 📋 IRS 990 FORMS (Free but complex)
   • Schedule I contains detailed grants data
   • 1-2 year delay in availability
   • Sources: ProPublica, IRS AWS S3, GuideStar

3. 🔍 MANUAL RESEARCH (Free but time-intensive)
   • Foundation websites, news sources, annual reports

📈 RECOMMENDED WORKFLOW:
1. Use generated research_plan.csv to prioritize targets
2. Start with largest foundations in your geographic area
3. Check foundation websites for published grants lists
4. Search local news for grant announcements
5. Use collection_template.csv to record findings

🛠️ AUTOMATION OPPORTUNITIES:
• Web scraping foundation websites
• News API integration for grant announcements
• IRS 990 XML parsing (framework provided)
"""
        print(guide_text)


class ComprehensiveDataCollector:
    """Complete data collector combining all functionality"""
    
    def __init__(self):
        self.base_url = 'https://projects.propublica.org/nonprofits/api/v2'
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Nonprofit Research Tool 1.0'})
        self.delay = float(os.getenv('API_DELAY_SECONDS', 1))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Initialize components
        self.irs_parser = IRS990Parser()
        self.grants_guide = GrantsResearchGuide()
        
        # Create data directory
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
    
    def search_nonprofits(self, search_params: List[Dict], max_pages: int = 5) -> List[Dict]:
        """Search for nonprofits using ProPublica API with flexible parameters"""
        all_results = []
        
        for params in tqdm(search_params, desc="Searching nonprofits"):
            page = 0
            has_more = True
            
            while has_more and page < max_pages:
                try:
                    if page > 0:
                        params['page'] = page
                    
                    response = self.session.get(
                        f"{self.base_url}/search.json",
                        params=params,
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        organizations = data.get('organizations', [])
                        
                        if organizations:
                            all_results.extend(organizations)
                            page += 1
                            has_more = len(organizations) == 25  # ProPublica returns 25 per page
                        else:
                            has_more = False
                    else:
                        logger.warning(f"API request failed with status {response.status_code}")
                        has_more = False
                        
                except Exception as e:
                    logger.error(f"Error in search: {e}")
                    has_more = False
                
                time.sleep(self.delay)
        
        return all_results
    
    def collect_donor_advised_funds(self) -> pd.DataFrame:
        """Collect donor-advised funds data"""
        logger.info("🎯 Collecting donor-advised funds...")
        
        search_params = [
            # NTEE codes for donor-advised funds
            {'search[0][field]': 'ntee', 'search[0][operator]': 'is', 'search[0][value]': code}
            for code in ['T20', 'T22', 'T23', 'T31']
        ] + [
            # Name pattern searches
            {'search[0][field]': 'name', 'search[0][operator]': 'contains', 'search[0][value]': pattern}
            for pattern in ['donor advised', 'community foundation', 'foundation fund', 'giving fund']
        ]
        
        all_organizations = self.search_nonprofits(search_params)
        processed_orgs = self._process_organizations(all_organizations, 'Donor Advised Fund')
        
        df = pd.DataFrame(processed_orgs)
        if not df.empty:
            df = df.drop_duplicates(subset=['ein'])
            logger.info(f"✅ Collected {len(df)} unique donor-advised funds")
        
        return df
    
    def collect_private_foundations(self) -> pd.DataFrame:
        """Collect private foundations data"""
        logger.info("🏛️ Collecting private foundations...")
        
        search_params = [
            {'search[0][field]': 'ntee', 'search[0][operator]': 'is', 'search[0][value]': code}
            for code in ['T10', 'T11', 'T12', 'T20', 'T21', 'T22', 'T30', 'T31']
        ]
        
        all_organizations = self.search_nonprofits(search_params)
        
        # Filter for foundation-related organizations
        foundation_keywords = ['foundation', 'fund', 'trust', 'endowment', 'charitable']
        filtered_orgs = [
            org for org in all_organizations
            if any(keyword in org.get('name', '').lower() for keyword in foundation_keywords) or
               any(keyword in org.get('classification', '').lower() for keyword in foundation_keywords)
        ]
        
        processed_orgs = self._process_organizations(filtered_orgs, 'Private Foundation')
        
        df = pd.DataFrame(processed_orgs)
        if not df.empty:
            df = df.drop_duplicates(subset=['ein'])
            logger.info(f"✅ Collected {len(df)} unique private foundations")
        
        return df
    
    def _process_organizations(self, organizations: List[Dict], org_type: str) -> List[Dict]:
        """Process raw organization data into standardized format"""
        collection_timestamp = datetime.now()
        
        processed = []
        for org in organizations:
            processed.append({
                'ein': org.get('ein', ''),
                'name': org.get('name', ''),
                'city': org.get('city', ''),
                'state': org.get('state', ''),
                'ntee_code': org.get('ntee_code', ''),
                'classification': org.get('classification', ''),
                'organization_type': org_type,
                'ruling_date': org.get('ruling_date', ''),
                'revenue': org.get('revenue_amount', 0) or 0,  # Handle None values
                'assets': org.get('asset_amount', 0) or 0,    # Handle None values
                'collection_date': collection_timestamp.strftime('%Y-%m-%d'),
                'collection_timestamp': collection_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return processed
    
    def save_datasets(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame):
        """Save all datasets to files"""
        # Save main datasets
        for df, name in [(donor_funds_df, 'donor_advised_funds'), (foundations_df, 'private_foundations')]:
            if not df.empty:
                df.to_csv(self.data_dir / f'{name}.csv', index=False)
                df.to_excel(self.data_dir / f'{name}.xlsx', index=False)
                logger.info(f"💾 Saved {len(df)} {name.replace('_', ' ')}")
        
        # Generate research resources
        if not foundations_df.empty:
            research_plan = self.grants_guide.generate_research_plan(foundations_df)
            research_plan.to_csv(self.data_dir / 'grants_research_plan.csv', index=False)
            logger.info(f"📋 Generated research plan with {len(research_plan)} targets")
            
            collection_template = self.grants_guide.generate_collection_template()
            collection_template.to_csv(self.data_dir / 'grants_collection_template.csv', index=False)
            logger.info("📝 Generated grants collection template")
        
        # Save collection summary
        self._save_collection_summary(donor_funds_df, foundations_df)
    
    def _save_collection_summary(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame):
        """Save collection summary and statistics"""
        summary_file = self.data_dir / 'collection_summary.json'
        collection_timestamp = datetime.now()
        
        run_summary = {
            "timestamp": collection_timestamp.isoformat(),
            "human_timestamp": collection_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "donor_advised_funds_count": len(donor_funds_df),
            "private_foundations_count": len(foundations_df),
        }
        
        # Load or create summary data
        try:
            if summary_file.exists():
                with open(summary_file, 'r') as f:
                    summary_data = json.load(f)
                if 'runs' not in summary_data:
                    summary_data = {"runs": [summary_data] if summary_data else []}
            else:
                summary_data = {"runs": []}
        except Exception as e:
            logger.warning(f"Could not load existing summary: {e}. Creating new summary.")
            summary_data = {"runs": []}
        
        # Update summary
        summary_data["runs"].append(run_summary)
        summary_data.update({
            "last_updated": collection_timestamp.isoformat(),
            "total_runs": len(summary_data["runs"]),
            "latest_run": run_summary
        })
        
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        logger.info(f"📊 Updated collection summary (run #{summary_data['total_runs']})")
    
    def run_full_collection(self, include_research_guide: bool = True):
        """Run the complete data collection workflow"""
        logger.info("🚀 Starting comprehensive nonprofit data collection...")
        start_time = datetime.now()
        
        if include_research_guide:
            self.grants_guide.print_comprehensive_guide()
        
        # Collect organizational data
        donor_funds_df = self.collect_donor_advised_funds()
        foundations_df = self.collect_private_foundations()
        
        # Save all datasets
        self.save_datasets(donor_funds_df, foundations_df)
        
        # Print summary
        duration = datetime.now() - start_time
        self._print_completion_summary(donor_funds_df, foundations_df, duration)
    
    def _print_completion_summary(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, duration):
        """Print completion summary"""
        summary = f"""
================================================================================
🎉 COMPREHENSIVE DATA COLLECTION COMPLETE
================================================================================
⏱️  Total time: {duration}
📊 Donor-advised funds: {len(donor_funds_df)}
🏛️  Private foundations: {len(foundations_df)}

📁 Files saved to: /data
   • donor_advised_funds.csv/.xlsx (consolidated)
   • private_foundations.csv/.xlsx (consolidated)
   • grants_research_plan.csv (prioritized research targets)
   • grants_collection_template.csv (for manual data entry)
   • collection_summary.json (with run history)

🎯 NEXT STEPS:
   1. Review grants_research_plan.csv for systematic research approach
   2. Use grants_collection_template.csv to record found grants
   3. Consider subscribing to Candid Foundation Directory for complete grants data

✅ Successfully collected comprehensive nonprofit organization data!
================================================================================"""
        print(summary)


def main():
    """Main execution function"""
    import sys
    
    skip_guide = '--skip-guide' in sys.argv
    include_research_guide = not skip_guide
    
    collector = ComprehensiveDataCollector()
    collector.run_full_collection(include_research_guide=include_research_guide)


if __name__ == "__main__":
    main()
        
        all_grants = []
        
        # Focus on larger organizations more likely to have grants data
        sorted_orgs = sorted(organizations, key=lambda x: x.get('revenue', 0), reverse=True)
        target_orgs = sorted_orgs[:50]  # Top 50 by revenue
        
        for org in tqdm(target_orgs, desc="Processing 990 forms"):
            ein = org.get('ein')
            if not ein:
                continue
            
            try:
                # Get available 990 files
                available_files = self.irs_parser.get_990_xml_from_propublica(ein)
                
                # Process most recent filings
                for filing in available_files[:2]:  # Process up to 2 most recent
                    if filing.get('pdf_url'):
                        # Note: This would require PDF parsing, which is complex
                        # For now, we'll create research resources
                        pass
                
            except Exception as e:
                logger.error(f"Error processing 990 for EIN {ein}: {e}")
            
            time.sleep(self.delay)
        
        # For now, return empty DataFrame and focus on research resources
        return pd.DataFrame(all_grants)
    
    def save_datasets(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, grants_df: pd.DataFrame):
        """Save all datasets to files"""
        collection_timestamp = datetime.now()
        
        # Save donor-advised funds
        if not donor_funds_df.empty:
            donor_funds_df.to_csv(self.data_dir / 'donor_advised_funds.csv', index=False)
            donor_funds_df.to_excel(self.data_dir / 'donor_advised_funds.xlsx', index=False)
            logger.info(f"💾 Saved {len(donor_funds_df)} donor-advised funds")
        
        # Save private foundations
        if not foundations_df.empty:
            foundations_df.to_csv(self.data_dir / 'private_foundations.csv', index=False)
            foundations_df.to_excel(self.data_dir / 'private_foundations.xlsx', index=False)
            logger.info(f"💾 Saved {len(foundations_df)} private foundations")
        
        # Save grants data
        if not grants_df.empty:
            grants_df.to_csv(self.data_dir / 'grants.csv', index=False)
            grants_df.to_excel(self.data_dir / 'grants.xlsx', index=False)
            logger.info(f"💾 Saved {len(grants_df)} grants")
        
        # Generate research resources
        if not foundations_df.empty:
            research_plan = self.grants_guide.generate_research_plan(foundations_df)
            research_plan.to_csv(self.data_dir / 'grants_research_plan.csv', index=False)
            logger.info(f"📋 Generated research plan with {len(research_plan)} targets")
            
            collection_template = self.grants_guide.generate_collection_template()
            collection_template.to_csv(self.data_dir / 'grants_collection_template.csv', index=False)
            logger.info("📝 Generated grants collection template")
        
        # Save collection summary
        self._save_collection_summary(donor_funds_df, foundations_df, grants_df, collection_timestamp)
    
    def _save_collection_summary(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, 
                                grants_df: pd.DataFrame, collection_timestamp: datetime):
        """Save collection summary and statistics"""
        summary_file = self.data_dir / 'collection_summary.json'
        
        run_summary = {
            "timestamp": collection_timestamp.isoformat(),
            "human_timestamp": collection_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "donor_advised_funds_count": len(donor_funds_df),
            "private_foundations_count": len(foundations_df),
            "total_grants": len(grants_df),
            "total_grant_amount": float(grants_df['amount'].sum()) if not grants_df.empty and 'amount' in grants_df.columns else 0
        }
        
        # Load existing summary or create new one
        if summary_file.exists():
            try:
                with open(summary_file, 'r') as f:
                    summary_data = json.load(f)
                if 'runs' not in summary_data:
                    summary_data = {"runs": [summary_data]}
            except Exception as e:
                logger.warning(f"Could not load existing summary: {e}. Creating new summary.")
                summary_data = {"runs": []}
        else:
            summary_data = {"runs": []}
        
        # Add new run to history
        summary_data["runs"].append(run_summary)
        
        # Update overall summary
        summary_data["last_updated"] = collection_timestamp.isoformat()
        summary_data["total_runs"] = len(summary_data["runs"])
        summary_data["latest_run"] = run_summary
        
        # Save updated summary
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        logger.info(f"📊 Updated collection summary (run #{summary_data['total_runs']})")
    
    def run_full_collection(self, include_grants: bool = False, include_research_guide: bool = True):
        """Run the complete data collection workflow"""
        logger.info("🚀 Starting comprehensive nonprofit data collection...")
        start_time = datetime.now()
        
        # Print grants research guide if requested
        if include_research_guide:
            self.grants_guide.print_comprehensive_guide()
        
        # Collect organizational data
        donor_funds_df = self.collect_donor_advised_funds()
        foundations_df = self.collect_private_foundations()
        
        # Collect grants data if requested
        if include_grants:
            logger.info("📊 Enhanced grants collection enabled")
            all_organizations = self.donor_advised_funds + self.private_foundations
            grants_df = self.collect_grants_from_irs_990(all_organizations)
        else:
            grants_df = pd.DataFrame()
        
        # Save all datasets
        self.save_datasets(donor_funds_df, foundations_df, grants_df)
        
        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        self._print_completion_summary(donor_funds_df, foundations_df, grants_df, duration, include_grants)
    
    def _print_completion_summary(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, 
                                 grants_df: pd.DataFrame, duration, include_grants: bool):
        """Print completion summary"""
        print(f"\n{'='*80}")
        print(f"🎉 COMPREHENSIVE DATA COLLECTION COMPLETE")
        print(f"{'='*80}")
        print(f"⏱️  Total time: {duration}")
        print(f"📊 Donor-advised funds: {len(donor_funds_df)}")
        print(f"🏛️  Private foundations: {len(foundations_df)}")
        print(f"💰 Grant records: {len(grants_df)}")
        
        if not grants_df.empty:
            if 'amount' in grants_df.columns:
                print(f"💵 Total grant amount: ${grants_df['amount'].sum():,.2f}")
                print(f"📈 Average grant size: ${grants_df['amount'].mean():,.2f}")
                print(f"🏆 Largest grant: ${grants_df['amount'].max():,.2f}")
        
        print(f"\n📁 Files saved to: /data")
        print(f"   • donor_advised_funds.csv/.xlsx (consolidated)")
        print(f"   • private_foundations.csv/.xlsx (consolidated)")
        if not grants_df.empty:
            print(f"   • grants.csv/.xlsx (consolidated)")
        print(f"   • grants_research_plan.csv (prioritized research targets)")
        print(f"   • grants_collection_template.csv (for manual data entry)")
        print(f"   • collection_summary.json (with run history)")
        
        print(f"\n🎯 NEXT STEPS:")
        print(f"   1. Review grants_research_plan.csv for systematic research approach")
        print(f"   2. Use grants_collection_template.csv to record found grants")
        print(f"   3. Open analyze_data.ipynb for comprehensive data analysis")
        print(f"   4. Consider subscribing to Candid Foundation Directory for complete grants data")
        
        print(f"\n✅ Successfully collected comprehensive nonprofit organization data!")
        print(f"   Data includes collection timestamps for tracking changes over time.")
        print(f"   Each run updates the consolidated files and maintains full history.")
        print(f"{'='*80}")


def main():
    """Main execution function"""
    import sys
    
    # Parse command line arguments
    include_grants = '--include-grants' in sys.argv or '--grants' in sys.argv
    skip_guide = '--skip-guide' in sys.argv
    include_research_guide = not skip_guide
    
    if include_grants:
        print("🚀 Enhanced grants research tools enabled!")
        print("📋 This will create comprehensive research resources for grants data")
        print("🎯 You'll get prioritized foundation lists and collection templates")
        print()
    
    collector = ComprehensiveDataCollector()
    collector.run_full_collection(
        include_grants=include_grants, 
        include_research_guide=include_research_guide
    )


if __name__ == "__main__":
    main()
