import requests
import os
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import glob
from tqdm import tqdm

def download_file(url, folder):
    """Downloads a file from a URL and saves it to a folder."""
    os.makedirs(folder, exist_ok=True)
    filename = url.split('/')[-1]
    filepath = os.path.join(folder, filename)
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def extract_zip(filepath, extract_to='data/xmls'):
    """Extracts a zip file and removes it."""
    if not filepath or not zipfile.is_zipfile(filepath):
        return
    
    os.makedirs(extract_to, exist_ok=True)

    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            for member in zip_ref.infolist():
                try:
                    zip_ref.extract(member, extract_to)
                except Exception as e:
                    print(f"Could not extract {member.filename} from {os.path.basename(filepath)}. Error: {e}")
        os.remove(filepath)
    except zipfile.BadZipFile as e:
        print(f"Error reading zip file {filepath}: {e}")

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

def process_xml_files():
    """Processes all XML files in the data/xmls directory and saves the data to a CSV file."""
    all_grants = []
    xml_files = glob.glob('data/xmls/*.xml')

    with ThreadPoolExecutor() as executor:
        results = executor.map(parse_grant_data, xml_files)
        for result in tqdm(results, total=len(xml_files), desc="Parsing XML files"):
            all_grants.extend(result)

    if all_grants:
        grants_df = pd.DataFrame(all_grants)
        output_path = 'data/parsed_grants.csv'
        grants_df.to_csv(output_path, index=False)
        print(f"\nSuccessfully parsed {len(grants_df)} grants and saved to {output_path}")
    else:
        print("No grant data was parsed.")

if __name__ == "__main__":
    download_and_extract_data()
    process_xml_files()

