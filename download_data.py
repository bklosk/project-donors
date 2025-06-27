import requests
import os
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

def download_file(url, folder='data'):
    """Downloads a file from a URL and saves it to a folder."""
    if not os.path.exists(folder):
        os.makedirs(folder)
    filename = url.split('/')[-1]
    filepath = os.path.join(folder, filename)
    print(f"Downloading {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"{filename} downloaded.")
        return filepath
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def extract_zip(filepath, extract_to='data'):
    """Extracts a zip file and removes it."""
    if filepath and zipfile.is_zipfile(filepath):
        print(f"Extracting {os.path.basename(filepath)}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        os.remove(filepath)
        print(f"{os.path.basename(filepath)} extracted and removed.")

def download_and_extract_data():
    """
    Downloads and extracts IRS Form 990 data in parallel.
    """
    index_url = "https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv"
    zip_urls = [
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_05A.zip",
    ]
    
    # Download index file separately
    download_file(index_url)

    # Download and extract ZIP files in parallel
    with ThreadPoolExecutor() as executor:
        zip_filepaths = list(executor.map(download_file, zip_urls))
        executor.map(extract_zip, zip_filepaths)

    print("All files downloaded and extracted.")

def check_data_format(xml_file):
    """
    Parses an XML file to determine if it contains grant data.
    This is a basic implementation and might need to be adjusted based on the actual XML structure.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        # This is a hypothetical path. You will need to inspect the XML to find the correct path.
        # For example, searching for tags like 'Grant' or 'Contribution'.
        grants = root.findall('.//GrantOrContributionAmt') 
        if grants:
            print(f"{xml_file}: Found {len(grants)} grant/contribution records.")
            return True
        else:
            print(f"{xml_file}: No grant/contribution records found with the specified path.")
            return False
    except ET.ParseError as e:
        print(f"Error parsing {xml_file}: {e}")
        return False

def convert_xml_to_csv(xml_file, csv_file):
    """
    This is a placeholder function to illustrate how you might convert XML to CSV.
    The implementation will heavily depend on the XML schema.
    """
    # try:
    #     tree = ET.parse(xml_file)
    #     root = tree.getroot()
    #     
    #     # This is a hypothetical data extraction logic.
    #     data = []
    #     for item in root.findall('.//SomeItem'): # Replace with actual item tag
    #         record = {
    #             'field1': item.find('Field1').text, # Replace with actual field tags
    #             'field2': item.find('Field2').text,
    #         }
    #         data.append(record)
    #     
    #     df = pd.DataFrame(data)
    #     df.to_csv(csv_file, index=False)
    #     print(f"Converted {xml_file} to {csv_file}")
    #
    # except Exception as e:
    #     print(f"Could not convert {xml_file} to CSV: {e}")
    pass


if __name__ == "__main__":
    download_and_extract_data()
    
    # After downloading, you can uncomment the following lines to analyze the data.
    # print("\nChecking data format...")
    # for file in os.listdir('data'):
    #     if file.endswith('.xml'):
    #         check_data_format(os.path.join('data', file))
    #         # convert_xml_to_csv(os.path.join('data', file), os.path.join('data', file.replace('.xml', '.csv')))

