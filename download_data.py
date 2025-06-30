import requests
import os
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET
import subprocess
from concurrent.futures import ThreadPoolExecutor
import glob
from tqdm import tqdm
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file(url, folder, retries=3, backoff_factor=0.5):
    """Downloads a file from a URL and saves it to a folder with retries."""
    os.makedirs(folder, exist_ok=True)
    filename = url.split('/')[-1]
    filepath = os.path.join(folder, filename)

    if os.path.exists(filepath):
        # If the file is a zip, validate it. If it's not valid, remove and redownload.
        if filename.endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zf:
                    if zf.testzip() is not None:
                        logging.warning(f"Corrupt zip file detected: {filename}. Removing.")
                        os.remove(filepath)
                    else:
                        logging.info(f"Existing valid zip file found: {filename}. Skipping download.")
                        return filepath
            except zipfile.BadZipFile:
                logging.warning(f"Bad zip file detected: {filename}. Removing.")
                os.remove(filepath)
        else: # For non-zip files, assume existence means it's fine.
            logging.info(f"File already exists: {filename}. Skipping download.")
            return filepath

    for attempt in range(retries):
        try:
            logging.info(f"Downloading {url} (Attempt {attempt + 1}/{retries})")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Verify zip file integrity after download
            if filename.endswith('.zip'):
                try:
                    with zipfile.ZipFile(filepath, 'r') as zf:
                        if zf.testzip() is not None:
                            raise zipfile.BadZipFile("Corrupt zip file detected after download.")
                    logging.info(f"Successfully downloaded and verified {filename}")
                    return filepath
                except zipfile.BadZipFile as e:
                    logging.warning(f"Validation failed for {filename}: {e}. Deleting file.")
                    os.remove(filepath)
                    # Continue to next retry
                    continue 
            else:
                logging.info(f"Successfully downloaded {filename}")
                return filepath

        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading {url}: {e}")
        
        time.sleep(backoff_factor * (2 ** attempt))

    logging.error(f"Failed to download {url} after {retries} attempts.")
    return None

def extract_zip(filepath, extract_to='data/xmls'):
    """Extracts a zip file and removes it."""
    if not filepath or not os.path.exists(filepath):
        return
    
    os.makedirs(extract_to, exist_ok=True)

    try:
        # Using system's unzip command for better compatibility
        subprocess.run(
            ['unzip', '-o', filepath, '-d', extract_to],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        os.remove(filepath)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logging.warning(f"'unzip' failed for {filepath}: {e}. Trying '7z'.")
        try:
            # Fallback to 7z for wider format support
            subprocess.run(
                ['7z', 'x', f'-o{extract_to}', filepath],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            os.remove(filepath)
        except (subprocess.CalledProcessError, FileNotFoundError) as e2:
            logging.warning(f"'7z' also failed for {filepath}: {e2}. Falling back to zipfile.")
            # Fallback to zipfile if unzip and 7z are not available or fail
            try:
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                os.remove(filepath)
            except zipfile.BadZipFile as e_zip:
                logging.error(f"Error extracting {os.path.basename(filepath)}: Bad zip file - {e_zip}")
            except Exception as e_zip:
                logging.error(f"An unexpected error occurred during extraction of {os.path.basename(filepath)} with zipfile: {e_zip}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction of {os.path.basename(filepath)}: {e}")

def download_and_extract_data():
    """
    Downloads and extracts IRS Form 990 data in parallel.
    """
    index_urls = [
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/index_2024.csv",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/index_2023.csv",
    ]
    zip_urls = [
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_05A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_06A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_07A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_08A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_09A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_10A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_11A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_12A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_05A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_06A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_07A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_08A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_09A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_10A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_11A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_12A.zip",
    ]
    
    with ThreadPoolExecutor() as executor:
        # Download index files
        list(tqdm(executor.map(lambda url: download_file(url, 'data'), index_urls), total=len(index_urls), desc="Downloading index files"))

        # Download and extract ZIP files in parallel
        zip_filepaths = list(tqdm(executor.map(lambda url: download_file(url, 'data/zips'), zip_urls), total=len(zip_urls), desc="Downloading zips"))
        list(tqdm(executor.map(extract_zip, zip_filepaths), total=len(zip_filepaths), desc="Extracting zips"))


def parse_grant_data(xml_file):
    """Parses an XML file to extract grant data."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        ns = {'irs': 'http://www.irs.gov/efile'}

        # General Filer Information
        filer_ein = root.find('.//irs:Filer/irs:EIN', ns).text
        filer_name_element = root.find('.//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt', ns)
        filer_name = filer_name_element.text if filer_name_element is not None else None
        
        # Return-level data
        return_type = root.find('.//irs:ReturnTypeCd', ns).text
        tax_period_end = root.find('.//irs:TaxPeriodEndDt', ns).text

        # Financial Data from IRS990PF
        grants_paid_amt_element = root.find('.//irs:GrantsAndContributionsPaidAmt', ns)
        grants_paid_amt = grants_paid_amt_element.text if grants_paid_amt_element is not None else 0

        # Schedule B Grant Information
        grants = []
        for grant_info in root.findall('.//irs:RecipientTable', ns):
            recipient_name_element = grant_info.find('.//irs:RecipientBusinessName/irs:BusinessNameLine1Txt', ns)
            recipient_name = recipient_name_element.text if recipient_name_element is not None else "Anonymous"
            
            address_element = grant_info.find('.//irs:RecipientUSAddress', ns)
            if address_element is not None:
                city = address_element.find('irs:CityNm', ns).text
                state = address_element.find('irs:StateAbbreviationCd', ns).text
                zip_code = address_element.find('irs:ZIPCd', ns).text
            else:
                city, state, zip_code = None, None, None

            amount_element = grant_info.find('.//irs:CashGrantAmt', ns)
            amount = int(amount_element.text) if amount_element is not None else 0
            
            purpose_element = grant_info.find('.//irs:PurposeOfGrantTxt', ns)
            purpose = purpose_element.text if purpose_element is not None else None

            grants.append({
                'FilerEIN': filer_ein,
                'FilerName': filer_name,
                'ReturnType': return_type,
                'TaxPeriodEnd': tax_period_end,
                'TotalGrantsPaid': grants_paid_amt,
                'RecipientName': recipient_name,
                'RecipientCity': city,
                'RecipientState': state,
                'RecipientZIP': zip_code,
                'GrantAmount': amount,
                'GrantPurpose': purpose
            })
        return grants
    except ET.ParseError as e:
        print(f"Could not parse {os.path.basename(xml_file)}: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred with {os.path.basename(xml_file)}: {e}")
        return []

def parse_filer_data(xml_file):
    """Parses an XML file to extract filer organization data."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        ns = {'irs': 'http://www.irs.gov/efile'}

        # General Filer Information
        filer_ein = root.find('.//irs:Filer/irs:EIN', ns)
        filer_ein = filer_ein.text if filer_ein is not None else None
        
        filer_name_element = root.find('.//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt', ns)
        filer_name = filer_name_element.text if filer_name_element is not None else None
        
        # Address information
        address_element = root.find('.//irs:Filer/irs:USAddress', ns)
        if address_element is not None:
            address_line1 = address_element.find('irs:AddressLine1Txt', ns)
            address_line1 = address_line1.text if address_line1 is not None else None
            
            city = address_element.find('irs:CityNm', ns)
            city = city.text if city is not None else None
            
            state = address_element.find('irs:StateAbbreviationCd', ns)
            state = state.text if state is not None else None
            
            zip_code = address_element.find('irs:ZIPCd', ns)
            zip_code = zip_code.text if zip_code is not None else None
        else:
            address_line1, city, state, zip_code = None, None, None, None

        # Return-level data
        return_type = root.find('.//irs:ReturnTypeCd', ns)
        return_type = return_type.text if return_type is not None else None
        
        tax_period_begin = root.find('.//irs:TaxPeriodBeginDt', ns)
        tax_period_begin = tax_period_begin.text if tax_period_begin is not None else None
        
        tax_period_end = root.find('.//irs:TaxPeriodEndDt', ns)
        tax_period_end = tax_period_end.text if tax_period_end is not None else None
        
        tax_year = root.find('.//irs:TaxYr', ns)
        tax_year = tax_year.text if tax_year is not None else None

        # Business Officer Information
        business_officer = root.find('.//irs:BusinessOfficerGrp/irs:PersonNm', ns)
        business_officer = business_officer.text if business_officer is not None else None
        
        officer_title = root.find('.//irs:BusinessOfficerGrp/irs:PersonTitleTxt', ns)
        officer_title = officer_title.text if officer_title is not None else None
        
        officer_phone = root.find('.//irs:BusinessOfficerGrp/irs:PhoneNum', ns)
        officer_phone = officer_phone.text if officer_phone is not None else None

        # Try to get organization type from various forms
        org_501c_type = None
        org_501_ind = None
        
        # Check IRS990 form
        form_990 = root.find('.//irs:IRS990', ns)
        if form_990 is not None:
            org_501c_type_elem = form_990.find('.//irs:Organization501c3Ind', ns)
            if org_501c_type_elem is not None:
                org_501c_type = "501c3"
            else:
                org_501c_type_elem = form_990.find('.//irs:Organization501cInd', ns)
                if org_501c_type_elem is not None:
                    org_501c_type = "501c"

        # Check IRS990PF form (Private Foundation)
        form_990pf = root.find('.//irs:IRS990PF', ns)
        if form_990pf is not None:
            org_501c_type = "990PF"

        # Check IRS990T form (Unrelated Business Income Tax)
        form_990t = root.find('.//irs:IRS990T', ns)
        if form_990t is not None:
            org_501c_type_elem = form_990t.find('.//irs:Organization501cTypeTxt', ns)
            if org_501c_type_elem is not None:
                org_501c_type = f"501{org_501c_type_elem.text}"

        # Financial information (if available)
        total_revenue = None
        total_expenses = None
        net_assets = None
        
        if form_990 is not None:
            total_revenue_elem = form_990.find('.//irs:TotalRevenueAmt', ns)
            total_revenue = int(total_revenue_elem.text) if total_revenue_elem is not None else None
            
            total_expenses_elem = form_990.find('.//irs:TotalExpensesAmt', ns)
            total_expenses = int(total_expenses_elem.text) if total_expenses_elem is not None else None
            
            net_assets_elem = form_990.find('.//irs:NetAssetsOrFundBalancesEOYAmt', ns)
            net_assets = int(net_assets_elem.text) if net_assets_elem is not None else None

        filer_data = {
            'EIN': filer_ein,
            'OrganizationName': filer_name,
            'AddressLine1': address_line1,
            'City': city,
            'State': state,
            'ZIPCode': zip_code,
            'ReturnType': return_type,
            'TaxPeriodBegin': tax_period_begin,
            'TaxPeriodEnd': tax_period_end,
            'TaxYear': tax_year,
            'BusinessOfficer': business_officer,
            'OfficerTitle': officer_title,
            'OfficerPhone': officer_phone,
            'Organization501cType': org_501c_type,
            'TotalRevenue': total_revenue,
            'TotalExpenses': total_expenses,
            'NetAssets': net_assets
        }
        
        return filer_data
    except ET.ParseError as e:
        print(f"Could not parse {os.path.basename(xml_file)}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred with {os.path.basename(xml_file)}: {e}")
        return None

def process_xml_files():
    """Processes all XML files in the data/xmls directory and saves the data to a CSV file."""
    all_grants = []
    all_filer_data = []
    xml_files = glob.glob('data/xmls/*.xml')

    with ThreadPoolExecutor() as executor:
        # Parse grant data
        results = executor.map(parse_grant_data, xml_files)
        for result in tqdm(results, total=len(xml_files), desc="Parsing grant data"):
            all_grants.extend(result)

        # Parse filer data
        results = executor.map(parse_filer_data, xml_files)
        for result in tqdm(results, total=len(xml_files), desc="Parsing filer data"):
            if result is not None:
                all_filer_data.append(result)

    # Save grant data to CSV
    if all_grants:
        grants_df = pd.DataFrame(all_grants)
        grants_output_path = 'data/parsed_grants.csv'
        grants_df.to_csv(grants_output_path, index=False)
        print(f"\nSuccessfully parsed {len(grants_df)} grants and saved to {grants_output_path}")
    else:
        print("No grant data was parsed.")

    # Save filer data to CSV
    if all_filer_data:
        filer_df = pd.DataFrame(all_filer_data)
        filer_output_path = 'data/parsed_filer_data.csv'
        filer_df.to_csv(filer_output_path, index=False)
        print(f"Successfully parsed {len(filer_df)} filer records and saved to {filer_output_path}")
    else:
        print("No filer data was parsed.")

if __name__ == "__main__":
    download_and_extract_data()
    process_xml_files()

