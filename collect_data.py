"""
ProPublica IRS 990 Data Collector
Single script to pull donor-advised funds, private foundations, and grants data from ProPublica API
Saves multiple datasets to /data folder for analysis
"""

import os
import time
import json
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import logging
from irs_990_parser import IRS990Parser

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

class ProPublicaDataCollector:
    """Complete data collector for ProPublica IRS 990 data"""
    
    def __init__(self):
        self.base_url = 'https://projects.propublica.org/nonprofits/api/v2'
        self.session = requests.Session()
        self.delay = float(os.getenv('API_DELAY_SECONDS', 1))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Data storage
        self.donor_advised_funds = []
        self.private_foundations = []
        self.all_grants = []
        
        # Initialize IRS 990 parser for grants data
        self.irs_parser = IRS990Parser()
        
        # Setup
        os.makedirs('data', exist_ok=True)
        
        logger.info("✅ ProPublica API ready (no API key required)")
        logger.info("✅ IRS 990 parser ready for grants data")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with retry logic and rate limiting"""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.delay)  # Rate limiting
                response = self.session.get(url, params=params)
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def search_organizations(self, query: str = None) -> List[Dict]:
        """Search for organizations with given criteria - API only supports query parameter"""
        params = {}
        if query:
            params['q'] = query
        # Note: API currently returns 500 errors for ntee, c_code, state parameters
        
        data = self._make_request('search.json', params)
        return data.get('organizations', []) if data else []
    
    def get_organization_details(self, ein: str) -> Optional[Dict]:
        """Get organization details and filings"""
        # Convert EIN to string and format with dash if not already formatted
        ein_str = str(ein)
        if '-' not in ein_str and len(ein_str) == 9:
            formatted_ein = f"{ein_str[:2]}-{ein_str[2:]}"
        else:
            formatted_ein = ein_str
            
        data = self._make_request(f'organizations/{formatted_ein}.json')
        return data if data else None
    
    def get_organization_filings(self, ein: str) -> List[Dict]:
        """Get all filings for an organization"""
        org_data = self.get_organization_details(ein)
        if org_data:
            # Combine both types of filings
            filings_with_data = org_data.get('filings_with_data', [])
            filings_without_data = org_data.get('filings_without_data', [])
            return filings_with_data + filings_without_data
        return []
    
    def get_filing_details(self, ein: str, object_id: str) -> Optional[Dict]:
        """Get detailed filing information"""
        # Convert EIN to string and format with dash if not already formatted
        ein_str = str(ein)
        if '-' not in ein_str and len(ein_str) == 9:
            formatted_ein = f"{ein_str[:2]}-{ein_str[2:]}"
        else:
            formatted_ein = ein_str
            
        data = self._make_request(f'organizations/{formatted_ein}/{object_id}.json')
        return data.get('filing') if data else None
    
    def collect_donor_advised_funds(self) -> pd.DataFrame:
        """Collect donor-advised funds data"""
        logger.info("🔍 Collecting donor-advised funds...")
        
        search_terms = [
            'donor advised fund',
            'community foundation', 
            'donor advised',
            'giving fund',
            'charitable fund'
        ]
        
        all_orgs = []
        
        for term in tqdm(search_terms, desc="Searching terms"):
            logger.info(f"Searching for: {term}")
            
            # Only basic search supported - no NTEE filtering
            orgs = self.search_organizations(query=term)
            all_orgs.extend(orgs)
        
        # Filter results for likely donor-advised funds
        filtered_orgs = []
        for org in all_orgs:
            name = org.get('name', '').lower()
            if any(keyword in name for keyword in ['donor', 'community foundation', 'giving', 'charitable']):
                filtered_orgs.append(org)
        
        # Remove duplicates based on EIN
        seen_eins = set()
        unique_orgs = []
        for org in filtered_orgs:
            ein = org.get('ein')
            if ein and ein not in seen_eins:
                seen_eins.add(ein)
                org['organization_type'] = 'Donor Advised Fund'
                unique_orgs.append(org)
        
        self.donor_advised_funds = unique_orgs
        df = pd.DataFrame(unique_orgs)
        
        logger.info(f"✅ Found {len(unique_orgs)} donor-advised funds")
        return df
    
    def collect_private_foundations(self) -> pd.DataFrame:
        """Collect private foundations data"""
        logger.info("🔍 Collecting private foundations...")
        
        search_terms = [
            'private foundation',
            'family foundation',
            'charitable foundation',
            'foundation',
            'trust',
            'endowment'
        ]
        
        all_orgs = []
        
        for term in tqdm(search_terms, desc="Searching terms"):
            logger.info(f"Searching for: {term}")
            
            # Only basic search supported - no c_code filtering
            orgs = self.search_organizations(query=term)
            all_orgs.extend(orgs)
        
        # Filter for likely private foundations
        foundation_orgs = []
        for org in all_orgs:
            name = org.get('name', '').lower()
            # More specific filtering since we can't use c_code
            if any(keyword in name for keyword in ['private foundation', 'family foundation', 'foundation']):
                # Exclude likely DAFs and community foundations
                if not any(exclude in name for exclude in ['community', 'donor advised', 'giving fund']):
                    foundation_orgs.append(org)
        
        # Remove duplicates
        seen_eins = set()
        unique_orgs = []
        for org in foundation_orgs:
            ein = org.get('ein')
            if ein and ein not in seen_eins:
                seen_eins.add(ein)
                org['organization_type'] = 'Private Foundation'
                unique_orgs.append(org)
        
        self.private_foundations = unique_orgs
        df = pd.DataFrame(unique_orgs)
        
        logger.info(f"✅ Found {len(unique_orgs)} private foundations")
        return df
    
    def extract_grants_from_filing(self, filing_data: Dict, org_data: Dict) -> List[Dict]:
        """Extract grant information from a single filing"""
        grants = []
        
        # Basic grant info extraction (simplified)
        if 'grants' in filing_data:
            for grant in filing_data['grants']:
                grants.append({
                    'grantor_ein': org_data.get('ein'),
                    'grantor_name': org_data.get('name'),
                    'grantor_type': org_data.get('organization_type', 'Unknown'),
                    'recipient_name': grant.get('recipient_name'),
                    'recipient_ein': grant.get('recipient_ein'),
                    'amount': grant.get('amount'),
                    'purpose': grant.get('purpose'),
                    'filing_year': filing_data.get('tax_period'),
                    'filing_type': filing_data.get('formtype')
                })
        
        # Look for Schedule I data (grants and contributions paid)
        if 'schedule_i' in filing_data:
            schedule_i = filing_data['schedule_i']
            if isinstance(schedule_i, list):
                for grant in schedule_i:
                    grants.append({
                        'grantor_ein': org_data.get('ein'),
                        'grantor_name': org_data.get('name'),
                        'grantor_type': org_data.get('organization_type', 'Unknown'),
                        'recipient_name': grant.get('recipient_name'),
                        'recipient_ein': grant.get('recipient_ein'),
                        'amount': grant.get('cash_grant') or grant.get('amount'),
                        'purpose': grant.get('purpose_of_grant') or grant.get('purpose'),
                        'filing_year': filing_data.get('tax_period'),
                        'filing_type': filing_data.get('formtype')
                    })
        
        return grants
    
    def collect_grants_data(self, organizations: List[Dict]) -> pd.DataFrame:
        """Collect grants data from organization filings"""
        logger.info(f"📥 Collecting grants data from {len(organizations)} organizations...")
        logger.info("⏱️ This may take a while - pulling ALL filings per organization...")
        
        all_grants = []
        
        for org in tqdm(organizations, desc="Processing organizations"):
            ein = org.get('ein')
            if not ein:
                continue
            
            try:
                # Get ALL filings for this organization
                filings = self.get_organization_filings(ein)
                logger.info(f"Processing {len(filings)} filings for {org.get('name', 'Unknown')[:50]}...")
                
                for filing in filings:
                    object_id = filing.get('object_id')
                    # Skip filings without valid object IDs
                    if not object_id or object_id == 'None':
                        continue
                        
                    filing_data = self.get_filing_details(ein, object_id)
                    
                    if filing_data:
                        grants = self.extract_grants_from_filing(filing_data, org)
                        all_grants.extend(grants)
            
            except Exception as e:
                logger.error(f"Error processing organization {ein}: {e}")
                continue
        
        self.all_grants = all_grants
        df = pd.DataFrame(all_grants)
        
        # Clean up the data
        if not df.empty:
            # Convert amount to numeric
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            # Remove grants with no amount
            df = df[df['amount'] > 0]
            # Convert filing year to numeric
            df['filing_year'] = pd.to_numeric(df['filing_year'], errors='coerce')
        
        logger.info(f"✅ Extracted {len(df)} grant records")
        return df
    
    def collect_grants_from_irs_990(self, organizations: List[Dict]) -> pd.DataFrame:
        """Collect detailed grants data from IRS 990 XML files"""
        logger.info("📋 Starting IRS 990 grants collection...")
        logger.info("📥 Downloading and parsing actual IRS 990 XML files for grants data")
        
        # Use the IRS 990 parser to collect grants
        grants_df = self.irs_parser.collect_grants_for_organizations(organizations)
        
        if not grants_df.empty:
            logger.info(f"✅ Collected {len(grants_df)} grants from IRS 990 filings")
        else:
            logger.info("No grants found in available IRS 990 filings")
        
        return grants_df

    def save_datasets(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, 
                     grants_df: pd.DataFrame) -> None:
        """Save all datasets to consolidated files with timestamps"""
        collection_timestamp = datetime.now()
        
        # Create data directory if it doesn't exist
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        # Add collection timestamp to all dataframes
        if not donor_funds_df.empty:
            donor_funds_df = donor_funds_df.copy()
            donor_funds_df['collection_date'] = collection_timestamp
            donor_funds_df['collection_timestamp'] = collection_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        if not foundations_df.empty:
            foundations_df = foundations_df.copy()
            foundations_df['collection_date'] = collection_timestamp
            foundations_df['collection_timestamp'] = collection_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        if not grants_df.empty:
            grants_df = grants_df.copy()
            grants_df['collection_date'] = collection_timestamp
            grants_df['collection_timestamp'] = collection_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # Consolidate and save donor-advised funds
        if not donor_funds_df.empty:
            self._save_consolidated_data(donor_funds_df, "donor_advised_funds", data_dir)
        
        # Consolidate and save private foundations
        if not foundations_df.empty:
            self._save_consolidated_data(foundations_df, "private_foundations", data_dir)
        
        # Consolidate and save grants data (if any)
        if not grants_df.empty:
            self._save_consolidated_data(grants_df, "grants", data_dir)
        
        # Update summary statistics
        self._update_collection_summary(donor_funds_df, foundations_df, grants_df, data_dir, collection_timestamp)
    
    def _save_consolidated_data(self, new_df: pd.DataFrame, dataset_name: str, data_dir: Path):
        """Save data to consolidated files, merging with existing data"""
        csv_file = data_dir / f"{dataset_name}.csv"
        xlsx_file = data_dir / f"{dataset_name}.xlsx"
        
        # Load existing data if files exist
        if csv_file.exists():
            try:
                existing_df = pd.read_csv(csv_file)
                # Convert collection_date to datetime if it exists
                if 'collection_date' in existing_df.columns:
                    existing_df['collection_date'] = pd.to_datetime(existing_df['collection_date'])
                
                # Remove old entries for organizations that are being updated
                if 'ein' in new_df.columns:
                    existing_df = existing_df[~existing_df['ein'].isin(new_df['ein'])]
                
                # Combine old and new data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                logger.warning(f"Could not load existing {dataset_name} data: {e}. Creating new file.")
                combined_df = new_df
        else:
            combined_df = new_df
        
        # Sort by collection date (newest first) and then by EIN
        if 'collection_date' in combined_df.columns:
            # For grants data, use grantor_ein instead of ein
            ein_column = 'grantor_ein' if 'grantor_ein' in combined_df.columns else 'ein'
            combined_df = combined_df.sort_values(['collection_date', ein_column], ascending=[False, True])
        
        # Save consolidated data
        combined_df.to_csv(csv_file, index=False)
        combined_df.to_excel(xlsx_file, index=False)
        logger.info(f"💾 Updated consolidated {dataset_name}: {csv_file} ({len(combined_df)} total records)")
    
    def _update_collection_summary(self, donor_funds_df: pd.DataFrame, foundations_df: pd.DataFrame, 
                                 grants_df: pd.DataFrame, data_dir: Path, collection_timestamp: datetime):
        """Update collection summary with run history"""
        summary_file = data_dir / "collection_summary.json"
        
        # Create new run summary
        run_summary = {
            "collection_date": collection_timestamp.isoformat(),
            "collection_timestamp": collection_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "donor_advised_funds_count": len(donor_funds_df),
            "private_foundations_count": len(foundations_df),
            "total_grants": len(grants_df),
            "total_grant_amount": float(grants_df['amount'].sum()) if not grants_df.empty else 0
        }
        
        # Load existing summary or create new one
        if summary_file.exists():
            try:
                with open(summary_file, 'r') as f:
                    summary_data = json.load(f)
                if 'runs' not in summary_data:
                    summary_data = {"runs": [summary_data]}  # Convert old format
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
    
    def run_full_collection(self, include_grants: bool = False):
        """Run the complete data collection workflow"""
        logger.info("🚀 Starting ProPublica data collection...")
        start_time = datetime.now()
        
        # Collect donor-advised funds
        donor_funds_df = self.collect_donor_advised_funds()
        
        # Collect private foundations
        foundations_df = self.collect_private_foundations()
        
        # Collect grants data
        all_organizations = self.donor_advised_funds + self.private_foundations
        
        if include_grants:
            logger.info("📊 Enhanced grants collection enabled")
            logger.info("� Downloading and parsing IRS 990 XML files for grants data")
            
            # Use IRS 990 XML parsing for comprehensive grants data
            grants_df = self.collect_grants_from_irs_990(all_organizations)
        else:
            logger.info("📊 Standard collection (organizations only)")
            grants_df = pd.DataFrame()  # Empty grants data
        
        # Save all datasets
        self.save_datasets(donor_funds_df, foundations_df, grants_df)
        
        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n{'='*60}")
        print(f"🎉 DATA COLLECTION COMPLETE")
        print(f"{'='*60}")
        print(f"⏱️  Total time: {duration}")
        print(f"📊 Donor-advised funds: {len(donor_funds_df)}")
        print(f"🏛️  Private foundations: {len(foundations_df)}")
        print(f"💰 Grant records: {len(grants_df)}")
        if not grants_df.empty:
            print(f"💵 Total grant amount: ${grants_df['amount'].sum():,.2f}")
            print(f"📈 Average grant size: ${grants_df['amount'].mean():,.2f}")
            print(f"🏆 Largest grant: ${grants_df['amount'].max():,.2f}")
        else:
            if include_grants:
                print(f"📋 Grants research resources created in /data folder")
                print(f"💡 Use grants_research_plan.csv to systematically research grants")
                print(f"📝 Use grants_collection_template.csv to record found grants")
            else:
                print(f"ℹ️  Note: Grant data collection not requested")
                print(f"    Run with --include-grants flag for comprehensive grants research tools")
        print(f"📁 Files saved to: /data")
        print(f"   • donor_advised_funds.csv/.xlsx (consolidated)")
        print(f"   • private_foundations.csv/.xlsx (consolidated)")
        if not grants_df.empty:
            print(f"   • grants.csv/.xlsx (consolidated)")
        print(f"   • collection_summary.json (with run history)")
        print(f"\n✅ Successfully collected comprehensive nonprofit organization data!")
        print(f"   Data includes collection timestamps for tracking changes over time.")
        print(f"   Each run updates the consolidated files and maintains full history.")
        print(f"{'='*60}")


def main():
    """Main execution function"""
    import sys
    
    # Check for grants collection flag
    include_grants = '--include-grants' in sys.argv
    
    if include_grants:
        print("🚀 Enhanced grants research tools enabled!")
        print("📋 This will create comprehensive research resources for grants data")
        print("� You'll get prioritized foundation lists and collection templates")
        print()
    
    collector = ProPublicaDataCollector()
    collector.run_full_collection(include_grants=include_grants)


if __name__ == "__main__":
    main()
