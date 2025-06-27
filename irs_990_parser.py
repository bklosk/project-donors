"""
IRS 990 Parser for Grants Data
Enhanced module to extract detailed grants information from IRS 990 forms
"""

import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import logging
from typing import Dict, List, Optional
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class IRS990Parser:
    """Parser for IRS 990 XML files to extract grants data"""
    
    def __init__(self):
        # IRS makes 990 data available in multiple ways:
        # 1. Annual AWS S3 dumps 
        # 2. Through the ProPublica API (with direct XML links)
        # 3. Through individual organization pages
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Nonprofit Research Tool 1.0'
        })
        
    def get_990_xml_from_propublica(self, ein: str) -> List[Dict]:
        """Get 990 XML URLs from ProPublica API"""
        available_files = []
        
        try:
            # Clean EIN format
            clean_ein = str(ein).replace('-', '')
            
            # Get organization data from ProPublica
            url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{clean_ein}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                org_data = data.get('organization', {})
                filings = data.get('filings_with_data', [])
                
                logger.info(f"Found {len(filings)} filings for EIN {ein}")
                
                # Look for filings that might have grants data
                for filing in filings:
                    # Focus on 990PF (private foundation) forms which are most likely to have grants
                    if filing.get('formtype') in ['990PF', '0']:  # 0 is sometimes used for 990PF
                        # Try to construct XML URL
                        year = filing.get('tax_prd_yr')
                        if year:
                            # ProPublica sometimes has direct XML links
                            xml_url = f"https://projects.propublica.org/nonprofits/api/v2/download/{clean_ein}_{year}.xml"
                            available_files.append({
                                'year': year,
                                'url': xml_url,
                                'filing_data': filing,
                                'form_type': '990PF' if filing.get('formtype') == '990PF' else '990'
                            })
                
            return available_files
            
        except Exception as e:
            logger.error(f"Error getting ProPublica data for {ein}: {e}")
            return []
        
    def get_available_990_files(self, ein: str, start_year: int = 2020, end_year: int = 2023) -> List[Dict]:
        """Find available IRS 990 files for an organization"""
        available_files = []
        
        # Format EIN for file naming (remove dash if present)
        clean_ein = str(ein).replace('-', '').zfill(9)
        
        # The actual IRS data is available at different locations
        # We'll check multiple patterns and sources
        
        for year in range(start_year, end_year + 1):
            # IRS 990 files are stored in various formats on AWS S3
            # The exact URL structure has changed over time
            
            # Try different naming patterns used by IRS
            file_patterns = [
                # Standard 990PF (Private Foundation) patterns
                f"{clean_ein}_{year}_990PF.xml",
                f"{clean_ein}_990PF_{year}.xml",
                f"{clean_ein}_{year}12_990PF.xml",
                
                # Standard 990 patterns
                f"{clean_ein}_{year}_990.xml",
                f"{clean_ein}_990_{year}.xml", 
                f"{clean_ein}_{year}12_990.xml",
                
                # Alternative patterns
                f"{clean_ein}_{year}_990EZ.xml",
                f"{clean_ein}_{year}_990N.xml"
            ]
            
            for pattern in file_patterns:
                # Check multiple potential S3 bucket locations
                urls_to_try = [
                    f"https://s3.amazonaws.com/irs-form-990/{year}/{pattern}",
                    f"https://irs-form-990.s3.amazonaws.com/{year}/{pattern}",
                ]
                
                for url in urls_to_try:
                    try:
                        # Use HEAD request to check if file exists
                        response = self.session.head(url, timeout=5)
                        if response.status_code == 200:
                            available_files.append({
                                'year': year,
                                'url': url,
                                'filename': pattern,
                                'form_type': '990PF' if '990PF' in pattern else ('990EZ' if '990EZ' in pattern else '990')
                            })
                            logger.info(f"Found 990 file for EIN {ein}, year {year}: {pattern}")
                            break  # Found file for this year, try next year
                    except Exception:
                        continue
                
                # If we found a file for this year, break out of pattern loop
                if available_files and available_files[-1]['year'] == year:
                    break
        
        if not available_files:
            logger.warning(f"No IRS 990 files found for EIN {ein} on AWS S3")
            logger.info("This could mean:")
            logger.info("  1. Organization doesn't file 990 forms")
            logger.info("  2. Files not yet uploaded to public AWS bucket")
            logger.info("  3. Different EIN format or filing requirements")
                    
        return available_files
    
    def parse_990_xml(self, xml_content: str) -> Dict:
        """Parse IRS 990 XML content to extract grants data"""
        try:
            root = ET.fromstring(xml_content)
            
            # Handle different XML namespaces
            namespaces = {
                'irs': 'http://www.irs.gov/efile',
                '': ''  # Default namespace
            }
            
            grants_data = {
                'grants': [],
                'total_grants': 0,
                'filing_year': None,
                'organization_name': None,
                'ein': None
            }
            
            # Extract basic organization info
            org_name_elem = root.find('.//BusinessName/BusinessNameLine1Txt') or \
                           root.find('.//BusinessNameLine1') or \
                           root.find('.//BusinessName')
            if org_name_elem is not None:
                grants_data['organization_name'] = org_name_elem.text
            
            ein_elem = root.find('.//EIN') or root.find('.//EIN/text()')
            if ein_elem is not None:
                grants_data['ein'] = ein_elem.text
            
            # Extract filing year
            tax_year_elem = root.find('.//TaxYear') or root.find('.//TaxYr')
            if tax_year_elem is not None:
                grants_data['filing_year'] = tax_year_elem.text
            
            # Look for Schedule I (Grants and Other Assistance)
            schedule_i_elements = (
                root.findall('.//GrantsAndOtherAssistanceToOrganizationsInUS') +
                root.findall('.//GrantsAndOtherAssistanceToIndividualsInUS') +
                root.findall('.//SupplementalInformationDetail') +
                root.findall('.//ScheduleI')
            )
            
            for schedule_elem in schedule_i_elements:
                grants = self._extract_grants_from_schedule_i(schedule_elem)
                grants_data['grants'].extend(grants)
            
            # Calculate total grants
            grants_data['total_grants'] = sum(
                grant.get('amount', 0) for grant in grants_data['grants']
            )
            
            logger.info(f"Extracted {len(grants_data['grants'])} grants from 990 filing")
            return grants_data
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error parsing 990 XML: {e}")
            return {}
    
    def _extract_grants_from_schedule_i(self, schedule_elem: ET.Element) -> List[Dict]:
        """Extract individual grants from Schedule I XML element"""
        grants = []
        
        # Look for grant recipient elements
        recipient_elements = (
            schedule_elem.findall('.//Recipient') +
            schedule_elem.findall('.//GrantOrContributionPdDuringYear') +
            schedule_elem.findall('.//GrantsAndOtherAssistanceToOrganization')
        )
        
        for recipient_elem in recipient_elements:
            grant = {}
            
            # Recipient name
            name_elem = (
                recipient_elem.find('.//RecipientBusinessName/BusinessNameLine1Txt') or
                recipient_elem.find('.//RecipientBusinessName/BusinessNameLine1') or
                recipient_elem.find('.//RecipientName') or
                recipient_elem.find('.//BusinessName')
            )
            if name_elem is not None:
                grant['recipient_name'] = name_elem.text
            
            # Recipient EIN
            ein_elem = (
                recipient_elem.find('.//RecipientEIN') or
                recipient_elem.find('.//EIN')
            )
            if ein_elem is not None:
                grant['recipient_ein'] = ein_elem.text
            
            # Grant amount
            amount_elem = (
                recipient_elem.find('.//CashGrantAmt') or
                recipient_elem.find('.//GrantOrContributionAmount') or
                recipient_elem.find('.//AmountOfCashGrant') or
                recipient_elem.find('.//Amount')
            )
            if amount_elem is not None:
                try:
                    grant['amount'] = float(amount_elem.text)
                except (ValueError, TypeError):
                    grant['amount'] = 0
            
            # Purpose of grant
            purpose_elem = (
                recipient_elem.find('.//PurposeOfGrantTxt') or
                recipient_elem.find('.//PurposeOfGrant') or
                recipient_elem.find('.//Purpose')
            )
            if purpose_elem is not None:
                grant['purpose'] = purpose_elem.text
            
            # Address information
            address_elem = recipient_elem.find('.//RecipientUSAddress') or \
                          recipient_elem.find('.//Address')
            if address_elem is not None:
                city_elem = address_elem.find('.//CityNm') or address_elem.find('.//City')
                state_elem = address_elem.find('.//StateAbbreviationCd') or address_elem.find('.//State')
                
                if city_elem is not None:
                    grant['recipient_city'] = city_elem.text
                if state_elem is not None:
                    grant['recipient_state'] = state_elem.text
            
            # Only add grants with meaningful data
            if grant.get('recipient_name') or grant.get('amount', 0) > 0:
                grants.append(grant)
        
        return grants
    
    def collect_grants_for_organization(self, ein: str, organization_name: str = None) -> List[Dict]:
        """Collect all grants data for a specific organization"""
        logger.info(f"🔍 Searching for grants data for EIN {ein}")
        
        all_grants = []
        
        try:
            # Clean EIN format
            clean_ein = str(ein).replace('-', '')
            
            # Get organization data from ProPublica API
            url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{clean_ein}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                org_data = data.get('organization', {})
                filings = data.get('filings_with_data', [])
                
                logger.info(f"Found {len(filings)} filings for {organization_name or 'EIN ' + ein}")
                
                # Process recent filings (last 3 years) - focus on private foundations
                recent_filings = sorted(filings, key=lambda x: x.get('tax_prd_yr', 0), reverse=True)[:5]
                
                for filing in recent_filings:
                    year = filing.get('tax_prd_yr')
                    form_type = filing.get('formtype', '')
                    
                    logger.info(f"Processing filing year {year}, form type: {form_type}")
                    
                    # Also try to extract summary grant information
                    summary_grants = self._extract_grants_from_filing_data(filing, org_data, ein, organization_name)
                    if summary_grants:
                        all_grants.extend(summary_grants)
                        logger.info(f"Found {len(summary_grants)} grants in {year} filing")
                    else:
                        logger.info(f"No grants extracted from {year} filing")
            
            # Remove duplicates
            seen = set()
            unique_grants = []
            for grant in all_grants:
                key = (grant.get('recipient_name'), grant.get('amount'), grant.get('filing_year'))
                if key not in seen:
                    seen.add(key)
                    unique_grants.append(grant)
            
            logger.info(f"✅ Collected {len(unique_grants)} total grants for {organization_name or ein}")
            return unique_grants
            
        except Exception as e:
            logger.error(f"Error collecting grants for {ein}: {e}")
            return []
    
    def _extract_grants_from_detailed_filing(self, clean_ein: str, object_id: str, year: int, ein: str, org_name: str) -> List[Dict]:
        """Try to extract grants from ProPublica's detailed filing data"""
        grants = []
        
        try:
            # ProPublica has detailed filing endpoints
            detail_url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{clean_ein}/filings/{object_id}.json"
            response = self.session.get(detail_url, timeout=15)
            
            if response.status_code == 200:
                filing_detail = response.json()
                
                # Look for grants data in the detailed filing
                # Check different possible grant fields
                grants_fields = [
                    'totalgrntstoorg',
                    'totalgrntstoindivs', 
                    'grntstoorg',
                    'grntstoindivs',
                    'totgrnts'
                ]
                
                for field in grants_fields:
                    amount = filing_detail.get(field, 0)
                    if amount and amount > 0:
                        grants.append({
                            'grantor_ein': ein,
                            'grantor_name': org_name,
                            'grantor_type': 'Private Foundation',
                            'recipient_name': f'Grant Recipients ({field})',
                            'recipient_ein': None,
                            'amount': amount,
                            'purpose': f'Grants as reported in {field}',
                            'filing_year': year,
                            'form_type': '990PF',
                            'source': 'ProPublica_Detail',
                            'recipient_city': None,
                            'recipient_state': None
                        })
                
                # Try to get actual grant schedule data if available
                if 'schedules' in filing_detail:
                    schedule_grants = self._extract_from_schedules(filing_detail['schedules'], ein, org_name, year)
                    grants.extend(schedule_grants)
                    
        except Exception as e:
            logger.debug(f"Could not get detailed filing data: {e}")
        
        return grants
    
    def _extract_from_schedules(self, schedules: Dict, ein: str, org_name: str, year: int) -> List[Dict]:
        """Extract grants from schedule data if available"""
        grants = []
        
        # Look for Schedule I (grants) data
        if 'schedule_i' in schedules:
            schedule_i = schedules['schedule_i']
            if isinstance(schedule_i, list):
                for grant_entry in schedule_i:
                    if isinstance(grant_entry, dict):
                        amount = grant_entry.get('amount') or grant_entry.get('cash_grant') or 0
                        if amount > 0:
                            grants.append({
                                'grantor_ein': ein,
                                'grantor_name': org_name,
                                'grantor_type': 'Private Foundation',
                                'recipient_name': grant_entry.get('recipient_name', 'Unknown Recipient'),
                                'recipient_ein': grant_entry.get('recipient_ein'),
                                'amount': amount,
                                'purpose': grant_entry.get('purpose', 'Grant purpose not specified'),
                                'filing_year': year,
                                'form_type': '990PF',
                                'source': 'ProPublica_Schedule',
                                'recipient_city': grant_entry.get('city'),
                                'recipient_state': grant_entry.get('state')
                            })
        
        return grants
    
    def _extract_grants_from_filing_data(self, filing: Dict, org_data: Dict, ein: str, org_name: str) -> List[Dict]:
        """Extract grants from ProPublica filing data"""
        grants = []
        
        # Debug: log what we're working with
        logger.info(f"Extracting grants from filing: form_type={filing.get('formtype')}, totfuncexpns={filing.get('totfuncexpns', 0)}")
        
        # For private foundations, most functional expenses are grants
        # Check various grant-related fields
        total_functional_expenses = filing.get('totfuncexpns', 0)
        
        # Private foundations are required to distribute ~5% of assets annually
        # Most of this goes to grants
        if total_functional_expenses > 0:
            # For private foundations (Form 990-PF), functional expenses are primarily grants
            form_type = str(filing.get('formtype', ''))
            logger.info(f"Form type: {form_type}, Total functional expenses: ${total_functional_expenses:,}")
            
            if form_type in ['990PF', '0']:  # 990-PF forms
                # Estimate grants as 80-95% of functional expenses for private foundations
                estimated_grants = int(total_functional_expenses * 0.9)
                
                logger.info(f"Creating estimated grant record: ${estimated_grants:,}")
                
                grants.append({
                    'grantor_ein': ein,
                    'grantor_name': org_name or org_data.get('name'),
                    'grantor_type': 'Private Foundation',
                    'recipient_name': 'Grant Recipients (Estimated from Functional Expenses)',
                    'recipient_ein': None,
                    'amount': estimated_grants,
                    'purpose': f'Estimated grants distribution (90% of ${total_functional_expenses:,} functional expenses)',
                    'filing_year': filing.get('tax_prd_yr'),
                    'form_type': '990PF',
                    'source': 'ProPublica_Estimated',
                    'recipient_city': None,
                    'recipient_state': None,
                    'notes': 'Estimated from total functional expenses - private foundations typically distribute 80-95% as grants'
                })
            else:
                logger.info(f"Skipping grants extraction - form type '{form_type}' not recognized as 990-PF")
        
        # Also check for any direct grant fields (though these seem to be 0 in the API)
        direct_grant_fields = [
            'totalgrntstoorg',
            'totalgrntstoindivs', 
            'grntstoorg',
            'grntstoindivs'
        ]
        
        for field in direct_grant_fields:
            amount = filing.get(field, 0)
            if amount and amount > 0:
                logger.info(f"Found direct grant field {field}: ${amount:,}")
                grants.append({
                    'grantor_ein': ein,
                    'grantor_name': org_name or org_data.get('name'),
                    'grantor_type': 'Private Foundation',
                    'recipient_name': f'Grant Recipients ({field})',
                    'recipient_ein': None,
                    'amount': amount,
                    'purpose': f'Grants as reported in {field}',
                    'filing_year': filing.get('tax_prd_yr'),
                    'form_type': filing.get('formtype'),
                    'source': 'ProPublica_Direct',
                    'recipient_city': None,
                    'recipient_state': None
                })
        
        logger.info(f"Extracted {len(grants)} grants from filing")
        return grants
    
    def _try_get_xml_grants(self, clean_ein: str, year: int, ein: str, org_name: str) -> List[Dict]:
        """Try to get detailed grants from XML files"""
        grants = []
        
        # Try different XML URL patterns
        xml_patterns = [
            f"https://projects.propublica.org/nonprofits/api/v2/download/{clean_ein}_{year}_990PF.xml",
            f"https://projects.propublica.org/nonprofits/api/v2/download/{clean_ein}_{year}.xml",
            f"https://s3.amazonaws.com/irs-form-990/{year}/{clean_ein}_{year}_990PF.xml",
            f"https://s3.amazonaws.com/irs-form-990/{year}/{clean_ein}_{year}12_990PF.xml"
        ]
        
        for xml_url in xml_patterns:
            try:
                response = self.session.get(xml_url, timeout=15)
                if response.status_code == 200 and response.content:
                    logger.info(f"Successfully downloaded XML for {ein} year {year}")
                    grants_data = self.parse_990_xml(response.text)
                    if grants_data.get('grants'):
                        for grant in grants_data['grants']:
                            grant.update({
                                'grantor_ein': ein,
                                'grantor_name': org_name,
                                'filing_year': year,
                                'source': 'IRS_990_XML'
                            })
                        grants.extend(grants_data['grants'])
                        break  # Found data, don't try other URLs
            except Exception as e:
                logger.debug(f"Could not get XML from {xml_url}: {e}")
                continue
        
        return grants
    
    def collect_grants_for_organizations(self, organizations: List[Dict]) -> pd.DataFrame:
        """Collect grants data for multiple organizations"""
        logger.info(f"📥 Collecting IRS 990 grants data for {len(organizations)} organizations...")
        logger.info("⚠️  This may take a while as we download and parse XML files...")
        
        all_grants = []
        
        for i, org in enumerate(organizations, 1):
            ein = org.get('strein') or org.get('ein')
            name = org.get('name')
            
            if not ein:
                continue
            
            logger.info(f"Processing {i}/{len(organizations)}: {name}")
            
            try:
                org_grants = self.collect_grants_for_organization(ein, name)
                all_grants.extend(org_grants)
                
                # Add a small delay to be respectful to servers
                import time
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error collecting grants for {name} (EIN: {ein}): {e}")
                continue
        
        # Convert to DataFrame
        if all_grants:
            df = pd.DataFrame(all_grants)
            
            # Clean up the data
            if 'amount' in df.columns:
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                df = df[df['amount'] > 0]  # Remove grants with no amount
            
            logger.info(f"✅ Collected {len(df)} grants from IRS 990 filings")
            return df
        else:
            logger.warning("No grants data found in IRS 990 filings")
            return pd.DataFrame()

def main():
    """Test the IRS 990 parser"""
    parser = IRS990Parser()
    
    # Test with a known foundation
    test_ein = "36-3330462"  # Crown Family Foundation
    grants = parser.collect_grants_for_organization(test_ein)
    
    if grants:
        df = pd.DataFrame(grants)
        print(f"Found {len(df)} grants:")
        print(df.head())
    else:
        print("No grants found")

if __name__ == "__main__":
    main()
