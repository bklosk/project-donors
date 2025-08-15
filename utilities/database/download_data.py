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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _txt(node):
    """Return node.text if node is not None, else None."""
    return node.text.strip() if node is not None and node.text is not None else None


def _first_text(elem, ns, paths):
    """Try a list of XPaths and return the first found text."""
    for p in paths:
        n = elem.find(p, ns)
        t = _txt(n)
        if t:
            return t
    return None


def _first_elem(elem, ns, paths):
    """Try a list of XPaths and return the first found element."""
    for p in paths:
        n = elem.find(p, ns)
        if n is not None:
            return n
    return None


def parse_int(value, default=None):
    """Best-effort integer parsing for varied XML numeric content.
    - Handles None/'' -> default
    - Handles commas and whitespace
    - Falls back to float cast, then int
    """
    if value in (None, ""):
        return default
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        try:
            return int(float(str(value).replace(",", "").strip()))
        except Exception:
            return default


def download_file(url, folder):
    """Downloads a file from a URL and saves it to a folder."""
    os.makedirs(folder, exist_ok=True)
    filename = url.split("/")[-1]
    filepath = os.path.join(folder, filename)

    if os.path.exists(filepath):
        logging.info(f"File already exists: {filename}. Skipping download.")
        return filepath

    try:
        logging.info(f"Downloading {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Successfully downloaded {filename}")
        return filepath
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")
        return None


def extract_zip(filepath, extract_to="data/xmls"):
    """Extracts a zip file using command-line tools and removes it."""
    if not filepath or not os.path.exists(filepath):
        return

    os.makedirs(extract_to, exist_ok=True)

    try:
        # Using system's unzip command for better compatibility
        subprocess.run(
            ["unzip", "-o", filepath, "-d", extract_to],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.remove(filepath)
        logging.info(f"Successfully extracted {os.path.basename(filepath)} with unzip.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning(
            f"'unzip' failed for {os.path.basename(filepath)}. Trying '7z'."
        )
        try:
            # Fallback to 7z for wider format support
            subprocess.run(
                ["7z", "x", f"-o{extract_to}", filepath, "-y"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            os.remove(filepath)
            logging.info(
                f"Successfully extracted {os.path.basename(filepath)} with 7z."
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            logging.error(
                f"Extraction failed for {os.path.basename(filepath)}. Both 'unzip' and '7z' failed."
            )


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
        list(
            tqdm(
                executor.map(lambda url: download_file(url, "data"), index_urls),
                total=len(index_urls),
                desc="Downloading index files",
            )
        )

        # Download and extract ZIP files in parallel
        zip_filepaths = list(
            tqdm(
                executor.map(lambda url: download_file(url, "data/zips"), zip_urls),
                total=len(zip_urls),
                desc="Downloading zips",
            )
        )
        list(
            tqdm(
                executor.map(extract_zip, zip_filepaths),
                total=len(zip_filepaths),
                desc="Extracting zips",
            )
        )


def parse_grant_data(xml_file):
    """Parses an XML file to extract grant data.

    Improvements:
    - Searches multiple vendor/year XPaths for grant rows
    - Normalizes recipient name (with BusinessNameLine2), purpose, and amounts (cash + non-cash)
    - Handles US and Foreign addresses consistently
    - Avoids crashes on missing fields
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        ns = {"irs": "http://www.irs.gov/efile"}

        # General Filer Information
        filer_ein = _first_text(root, ns, [".//irs:Filer/irs:EIN"])
        filer_name = _first_text(
            root,
            ns,
            [
                ".//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt",
                ".//irs:Filer/irs:Name/irs:BusinessNameLine1Txt",
            ],
        )

        # Return-level data
        return_type = _first_text(root, ns, [".//irs:ReturnTypeCd"])
        tax_period_end = _first_text(root, ns, [".//irs:TaxPeriodEndDt"])

        # Financial Data (best-effort)
        grants_paid_amt = _first_text(
            root,
            ns,
            [
                ".//irs:GrantsAndContributionsPaidAmt",
                ".//irs:TotalGrantOrContributionPdAmt",
            ],
        )
        grants_paid_amt = parse_int(grants_paid_amt, default=0)

        # Candidate containers for grant rows (varies by year/vendor)
        candidate_paths = [
            ".//irs:RecipientTable",  # common in some PF/vendor variants
            ".//irs:GrantOrContributionPdDurYrGrp",
            ".//irs:GrantsAndContributionsPdDurYrGrp",
            ".//irs:SupplementalInformationDetail/irs:GrantOrContributionPdDurYrGrp",
        ]

        # Collect all unique grant nodes from the candidates
        grant_nodes = []
        seen_ids = set()
        for p in candidate_paths:
            for n in root.findall(p, ns):
                nid = id(n)
                if nid not in seen_ids:
                    grant_nodes.append(n)
                    seen_ids.add(nid)

        def parse_name(node):
            # Prefer explicit person name when provided
            person = _first_text(
                node,
                ns,
                [
                    ".//irs:RecipientPersonNm",
                    ".//irs:RecipientNm",
                ],
            )
            # Business name lines (multiple possible containers)
            b1 = _first_text(
                node,
                ns,
                [
                    ".//irs:RecipientBusinessName/irs:BusinessNameLine1Txt",
                    ".//irs:RecipientNameBusiness/irs:BusinessNameLine1Txt",
                    ".//irs:BusinessName/irs:BusinessNameLine1Txt",
                ],
            )
            b2 = _first_text(
                node,
                ns,
                [
                    ".//irs:RecipientBusinessName/irs:BusinessNameLine2Txt",
                    ".//irs:RecipientNameBusiness/irs:BusinessNameLine2Txt",
                    ".//irs:BusinessName/irs:BusinessNameLine2Txt",
                ],
            )
            if person:
                return person, None, None
            if b1 or b2:
                full = " ".join(x for x in [b1, b2] if x)
                return full or "Anonymous", b1, b2
            return "Anonymous", None, None

        def parse_address(node):
            # Try US address variants first
            us_addr = _first_elem(
                node,
                ns,
                [
                    ".//irs:RecipientUSAddress",
                    ".//irs:USAddress",
                ],
            )
            if us_addr is not None:
                return {
                    "RecipientCity": _first_text(us_addr, ns, ["irs:CityNm"]),
                    "RecipientState": _first_text(
                        us_addr, ns, ["irs:StateAbbreviationCd"]
                    ),
                    "RecipientZIP": _first_text(us_addr, ns, ["irs:ZIPCd"]),
                    "RecipientProvince": None,
                    "RecipientCountry": "US",
                    "RecipientPostalCode": _first_text(us_addr, ns, ["irs:ZIPCd"]),
                }

            # Foreign address
            fr_addr = _first_elem(
                node,
                ns,
                [
                    ".//irs:RecipientForeignAddress",
                    ".//irs:ForeignAddress",
                ],
            )
            if fr_addr is not None:
                return {
                    "RecipientCity": _first_text(fr_addr, ns, ["irs:CityNm"]),
                    "RecipientState": None,
                    "RecipientZIP": None,
                    "RecipientProvince": _first_text(
                        fr_addr, ns, ["irs:ProvinceOrStateNm"]
                    ),
                    "RecipientCountry": _first_text(
                        fr_addr, ns, ["irs:CountryCd", "irs:CountryNm"]
                    ),
                    "RecipientPostalCode": _first_text(
                        fr_addr, ns, ["irs:ForeignPostalCd"]
                    ),
                }

            return {
                "RecipientCity": None,
                "RecipientState": None,
                "RecipientZIP": None,
                "RecipientProvince": None,
                "RecipientCountry": None,
                "RecipientPostalCode": None,
            }

        def parse_amounts(node):
            def to_int(s):
                return parse_int(s, default=0)

            cash = _first_text(
                node,
                ns,
                [
                    ".//irs:CashGrantAmt",
                    ".//irs:CashContributionAmt",
                ],
            )
            noncash = _first_text(
                node,
                ns,
                [
                    ".//irs:NonCashAssistanceAmt",
                    ".//irs:NoncashAssistanceAmt",
                    ".//irs:NonCashGrantAmt",
                    ".//irs:NoncashGrantAmt",
                ],
            )
            total = _first_text(
                node,
                ns,
                [
                    ".//irs:Amt",
                    ".//irs:GrantOrContributionAmt",
                ],
            )

            cash_i = to_int(cash)
            noncash_i = to_int(noncash)
            total_i = to_int(total)
            if (
                (cash_i or noncash_i)
                and isinstance(cash_i, int)
                and isinstance(noncash_i, int)
            ):
                total_i = cash_i + noncash_i
            return cash_i, noncash_i, total_i

        def parse_purpose(node):
            return _first_text(
                node,
                ns,
                [
                    ".//irs:PurposeOfGrantTxt",
                    ".//irs:GrantOrContributionPurposeTxt",
                    ".//irs:PurposeOfContributionTxt",
                    ".//irs:PurposeOfGrantDescriptionTxt",
                ],
            )

        grants = []
        for g in grant_nodes:
            rec_full, rec_line1, rec_line2 = parse_name(g)
            addr = parse_address(g)
            cash_amt, noncash_amt, total_amt = parse_amounts(g)
            purpose = parse_purpose(g)

            grants.append(
                {
                    "FilerEIN": filer_ein,
                    "FilerName": filer_name,
                    "ReturnType": return_type,
                    "TaxPeriodEnd": tax_period_end,
                    "TotalGrantsPaid": grants_paid_amt,
                    "RecipientName": rec_full,
                    "RecipientNameLine1": rec_line1,
                    "RecipientNameLine2": rec_line2,
                    **addr,
                    "GrantAmountCash": cash_amt,
                    "GrantAmountNonCash": noncash_amt,
                    "GrantAmountTotal": total_amt,
                    "GrantPurpose": purpose,
                }
            )
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
        ns = {"irs": "http://www.irs.gov/efile"}

        # General Filer Information
        filer_ein = root.find(".//irs:Filer/irs:EIN", ns)
        filer_ein = filer_ein.text if filer_ein is not None else None

        filer_name_element = root.find(
            ".//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt", ns
        )
        filer_name = filer_name_element.text if filer_name_element is not None else None

        # Address information
        address_element = root.find(".//irs:Filer/irs:USAddress", ns)
        if address_element is not None:
            address_line1 = address_element.find("irs:AddressLine1Txt", ns)
            address_line1 = address_line1.text if address_line1 is not None else None

            city = address_element.find("irs:CityNm", ns)
            city = city.text if city is not None else None

            state = address_element.find("irs:StateAbbreviationCd", ns)
            state = state.text if state is not None else None

            zip_code = address_element.find("irs:ZIPCd", ns)
            zip_code = zip_code.text if zip_code is not None else None
        else:
            address_line1, city, state, zip_code = None, None, None, None

        # Return-level data
        return_type = root.find(".//irs:ReturnTypeCd", ns)
        return_type = return_type.text if return_type is not None else None

        tax_period_begin = root.find(".//irs:TaxPeriodBeginDt", ns)
        tax_period_begin = (
            tax_period_begin.text if tax_period_begin is not None else None
        )

        tax_period_end = root.find(".//irs:TaxPeriodEndDt", ns)
        tax_period_end = tax_period_end.text if tax_period_end is not None else None

        tax_year = root.find(".//irs:TaxYr", ns)
        tax_year = tax_year.text if tax_year is not None else None

        # Business Officer Information
        business_officer = root.find(".//irs:BusinessOfficerGrp/irs:PersonNm", ns)
        business_officer = (
            business_officer.text if business_officer is not None else None
        )

        officer_title = root.find(".//irs:BusinessOfficerGrp/irs:PersonTitleTxt", ns)
        officer_title = officer_title.text if officer_title is not None else None

        officer_phone = root.find(".//irs:BusinessOfficerGrp/irs:PhoneNum", ns)
        officer_phone = officer_phone.text if officer_phone is not None else None

        # Try to get organization type from various forms
        org_501c_type = None
        org_501_ind = None

        # Check IRS990 form
        form_990 = root.find(".//irs:IRS990", ns)
        if form_990 is not None:
            org_501c_type_elem = form_990.find(".//irs:Organization501c3Ind", ns)
            if org_501c_type_elem is not None:
                org_501c_type = "501c3"
            else:
                org_501c_type_elem = form_990.find(".//irs:Organization501cInd", ns)
                if org_501c_type_elem is not None:
                    org_501c_type = "501c"

        # Check IRS990PF form (Private Foundation)
        form_990pf = root.find(".//irs:IRS990PF", ns)
        if form_990pf is not None:
            org_501c_type = "990PF"

        # Check IRS990T form (Unrelated Business Income Tax)
        form_990t = root.find(".//irs:IRS990T", ns)
        if form_990t is not None:
            org_501c_type_elem = form_990t.find(".//irs:Organization501cTypeTxt", ns)
            if org_501c_type_elem is not None:
                org_501c_type = f"501{org_501c_type_elem.text}"

        # Financial information (if available)
        total_revenue = None
        total_expenses = None
        net_assets = None

        if form_990 is not None:
            total_revenue_elem = form_990.find(".//irs:TotalRevenueAmt", ns)
            total_revenue = (
                parse_int(total_revenue_elem.text, default=None)
                if total_revenue_elem is not None
                else None
            )

            total_expenses_elem = form_990.find(".//irs:TotalExpensesAmt", ns)
            total_expenses = (
                parse_int(total_expenses_elem.text, default=None)
                if total_expenses_elem is not None
                else None
            )

            net_assets_elem = form_990.find(".//irs:NetAssetsOrFundBalancesEOYAmt", ns)
            net_assets = (
                parse_int(net_assets_elem.text, default=None)
                if net_assets_elem is not None
                else None
            )

        filer_data = {
            "EIN": filer_ein,
            "OrganizationName": filer_name,
            "AddressLine1": address_line1,
            "City": city,
            "State": state,
            "ZIPCode": zip_code,
            "ReturnType": return_type,
            "TaxPeriodBegin": tax_period_begin,
            "TaxPeriodEnd": tax_period_end,
            "TaxYear": tax_year,
            "BusinessOfficer": business_officer,
            "OfficerTitle": officer_title,
            "OfficerPhone": officer_phone,
            "Organization501cType": org_501c_type,
            "TotalRevenue": total_revenue,
            "TotalExpenses": total_expenses,
            "NetAssets": net_assets,
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
    all_pf_payout = []
    xml_files = glob.glob("data/xmls/*.xml")

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

        # Parse PF payout fields
        results = executor.map(parse_pf_payout_data, xml_files)
        for result in tqdm(
            results, total=len(xml_files), desc="Parsing PF payout fields"
        ):
            if result is not None:
                all_pf_payout.append(result)

    # Save grant data to CSV
    if all_grants:
        grants_df = pd.DataFrame(all_grants)
        grants_output_path = "data/parsed_grants.csv"
        grants_df.to_csv(grants_output_path, index=False)
        print(
            f"\nSuccessfully parsed {len(grants_df)} grants and saved to {grants_output_path}"
        )
    else:
        print("No grant data was parsed.")

    # Save filer data to CSV
    if all_filer_data:
        filer_df = pd.DataFrame(all_filer_data)
        filer_output_path = "data/parsed_filer_data.csv"
        filer_df.to_csv(filer_output_path, index=False)
        print(
            f"Successfully parsed {len(filer_df)} filer records and saved to {filer_output_path}"
        )
    else:
        print("No filer data was parsed.")

    # Save PF payout data to CSV
    if all_pf_payout:
        pf_df = pd.DataFrame(all_pf_payout)
        pf_output_path = "data/parsed_pf_payout.csv"
        pf_df.to_csv(pf_output_path, index=False)
        print(
            f"Successfully parsed {len(pf_df)} PF payout records and saved to {pf_output_path}"
        )
    else:
        print("No PF payout data was parsed.")


def parse_pf_payout_data(xml_file):
    """Extracts 990-PF payout-related fields and computes a PayoutPressureIndex.

    Outputs a single row per return with:
    - EIN, FilerName
    - TaxPeriodEnd (FYEnd)
    - DistributableAmount, QualifyingDistributions, UndistributedIncome
    - PayoutShortfall = max(DistributableAmount - QualifyingDistributions, 0)
    - PayoutPressureIndex = PayoutShortfall / DistributableAmount (if >0)
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        ns = {"irs": "http://www.irs.gov/efile"}

        # Only process 990-PF returns
        form_990pf = root.find(".//irs:IRS990PF", ns)
        if form_990pf is None:
            return None

        # Basic context
        filer_ein = _first_text(root, ns, [".//irs:Filer/irs:EIN"])  # EIN
        filer_name = _first_text(
            root,
            ns,
            [
                ".//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt",
                ".//irs:Filer/irs:Name/irs:BusinessNameLine1Txt",
            ],
        )
        fy_end = _first_text(root, ns, [".//irs:TaxPeriodEndDt"])  # Fiscal Year End
        # Derive FY end year/month to support estimate windows
        fy_end_year = None
        fy_end_month = None
        if fy_end and len(fy_end) >= 7:
            try:
                parts = fy_end.split("-")
                fy_end_year = int(parts[0])
                fy_end_month = int(parts[1])
            except Exception:
                fy_end_year = None
                fy_end_month = None

        # Candidate XPaths for each metric (varies by year/vendor)
        distro_paths = [
            ".//irs:DistributableAmount",  # some vendors
            ".//irs:DistributableAmt",
            ".//irs:DistributableAmountGrp/irs:DistributableAmt",
            ".//irs:MinimumInvestmentReturnGrp/irs:DistributableAmt",
            ".//irs:PartXIIDistributionGrp/irs:DistributableAmt",
        ]
        qual_paths = [
            ".//irs:QualifyingDistributionsAmt",
            ".//irs:QualifyingDistrsAmt",
            ".//irs:QualifyingDistributionGrp/irs:QualifyingDistributionsAmt",
            ".//irs:PartXIIDistributionGrp/irs:QualifyingDistributionsAmt",
        ]
        undistrib_paths = [
            ".//irs:UndistributedIncomeEOYAmt",
            ".//irs:UndistributedIncomeEndOfYrAmt",
            ".//irs:UndistributedIncomeAmt",
            ".//irs:PartXIIDistributionGrp/irs:UndistributedIncomeEOYAmt",
        ]

        def first_int(elem, paths):
            return parse_int(_first_text(elem, ns, paths), default=None)

        distributable = first_int(form_990pf, distro_paths)
        qualifying = first_int(form_990pf, qual_paths)
        undistrib = first_int(form_990pf, undistrib_paths)

        # Fallback: scan 990PF subtree for tags containing names, choose max value
        def fallback_scan(target_keywords):
            values = []
            for e in form_990pf.iter():
                if not isinstance(e.tag, str):
                    continue
                # localname after namespace
                local = e.tag.split("}")[-1]
                for kw in target_keywords:
                    if kw.lower() in local.lower():
                        v = parse_int(_txt(e), default=None)
                        if isinstance(v, int):
                            values.append(v)
                        break
            return max(values) if values else None

        if distributable is None:
            distributable = fallback_scan(["DistributableAmount", "DistributableAmt"])
        if qualifying is None:
            qualifying = fallback_scan(
                [
                    "QualifyingDistributionsAmt",
                    "QualifyingDistrsAmt",
                    "QualifyingDistribution",
                ]
            )
        if undistrib is None:
            undistrib = fallback_scan(
                [
                    "UndistributedIncomeEOYAmt",
                    "UndistributedIncome",
                ]
            )

        # Compute payout pressure
        shortfall = None
        ppi = None
        if isinstance(distributable, int) and distributable > 0:
            q = qualifying if isinstance(qualifying, int) else 0
            shortfall = max(distributable - q, 0)
            ppi = shortfall / distributable if distributable > 0 else None

        return {
            "EIN": filer_ein,
            "FilerName": filer_name,
            "TaxPeriodEnd": fy_end,
            "FYEndYear": fy_end_year,
            "FYEndMonth": fy_end_month,
            "DistributableAmount": distributable,
            "QualifyingDistributions": qualifying,
            "UndistributedIncome": undistrib,
            "PayoutShortfall": shortfall,
            "PayoutPressureIndex": ppi,
        }
    except ET.ParseError as e:
        print(f"Could not parse {os.path.basename(xml_file)}: {e}")
        return None
    except Exception as e:
        print(
            f"An unexpected error occurred with {os.path.basename(xml_file)} (PF payout): {e}"
        )
        return None


if __name__ == "__main__":
    download_and_extract_data()
    process_xml_files()
