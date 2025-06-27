# 📊 Project Donors: Comprehensive Nonprofit Data Collection

A comprehensive data collection system for analyzing donor-advised funds, private foundations, and their grant-making activities using public data sources.

## 🎯 Project Overview

This project provides tools to:

- **Collect organizational data** on donor-advised funds and private foundations
- **Generate research resources** for systematic grants data collection
- **Consolidate and track** data over time with timestamps
- **Provide comprehensive guides** for accessing grants information

## 📁 Project Structure

```
project-donors/
├── collect_data.py           # Main data collection script
├── grants_guide.py          # Comprehensive grants research guide
├── irs_990_parser.py        # IRS 990 XML parsing utilities
├── analyze_data.ipynb       # Jupyter notebook for data analysis
├── requirements.txt         # Python dependencies
├── data/                    # Generated data files
│   ├── donor_advised_funds.csv      # Consolidated donor-advised funds
│   ├── private_foundations.csv      # Consolidated private foundations
│   ├── grants_research_plan.csv     # Prioritized research targets
│   ├── grants_collection_template.csv # Template for manual collection
│   └── collection_summary.json      # Run history and metadata
└── README.md               # This file
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone or download the project
cd project-donors

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Basic Collection

```bash
# Collect organizational data only
python collect_data.py

# Include comprehensive grants research tools
python collect_data.py --include-grants
```

### 3. Analyze Data

```bash
# Open Jupyter notebook for analysis
jupyter notebook analyze_data.ipynb
```

## 📊 Data Sources

### Organizational Data (Automated)

- **ProPublica Nonprofit Explorer API**: Free access to IRS 990 data
- **Coverage**: 2M+ tax-exempt organizations
- **Data includes**: EIN, name, location, classification, filing status

### Grants Data (Research Tools Provided)

- **Candid Foundation Directory**: Most comprehensive (paid subscription)
- **IRS 990 Schedule I**: Free but requires manual processing
- **Foundation websites**: Direct from source but manual
- **News sources**: Grant announcements and press releases

## 📈 Generated Data Files

### Organizational Data

- `donor_advised_funds.csv/xlsx`: Complete list with metadata
- `private_foundations.csv/xlsx`: Complete list with metadata
- Both include collection timestamps for tracking changes

### Grants Research Resources

- `grants_research_plan.csv`: Prioritized list of foundations to research
- `grants_collection_template.csv`: Template for recording found grants
- Comprehensive research guide with URLs and search strategies

### Metadata

- `collection_summary.json`: Run history, statistics, and timestamps

## 🔍 Key Features

### Smart Data Collection

- **Rate limiting**: Respectful API usage with delays
- **Deduplication**: Automatic removal of duplicate entries
- **Error handling**: Robust retry logic and error recovery
- **Progress tracking**: Real-time progress bars and logging

### Consolidated Files

- **No timestamp pollution**: Single files updated each run
- **Change tracking**: Collection timestamps for trend analysis
- **Format flexibility**: Both CSV and Excel outputs
- **Incremental updates**: Only new/changed data is processed

### Comprehensive Grants Research

- **Prioritized targets**: Foundations ranked by research potential
- **Research URLs**: Direct links to foundation profiles
- **Search strategies**: Pre-built search queries for efficiency
- **Collection templates**: Standardized format for data entry

## 🎯 Grants Data Collection Strategy

### 🥇 Immediate Approach (Free)

1. Use generated research plan (`grants_research_plan.csv`)
2. Visit foundation websites for published grants lists
3. Search news sources for grant announcements
4. Record findings in collection template

### 🥈 Thorough Approach (Some Cost)

1. Download IRS 990 forms from ProPublica
2. Extract Schedule I grants data (manual or automated)
3. Focus on largest foundations first
4. Build comprehensive database over time

### 🥉 Professional Approach (Paid)

1. Subscribe to Candid Foundation Directory ($179+/month)
2. Export grants data for target foundations
3. Combine with organizational data from this project
4. Most complete and current information available

## 📊 Sample Data Analysis

The included Jupyter notebook demonstrates:

- Geographic distribution of foundations
- Analysis by foundation type and size
- Grant-making patterns and trends
- Visualization of the nonprofit ecosystem

## 🔧 Advanced Usage

### Custom Search Terms

Modify search terms in `collect_data.py`:

```python
donor_fund_terms = ['your', 'custom', 'terms']
foundation_terms = ['your', 'foundation', 'terms']
```

### Data Export

```python
import pandas as pd

# Load consolidated data
df = pd.read_csv('data/donor_advised_funds.csv')

# Filter by state
ca_funds = df[df['state'] == 'CA']

# Export subset
ca_funds.to_csv('california_funds.csv', index=False)
```

### Research Automation

Use the research plan for systematic data collection:

```python
import pandas as pd

# Load research plan
plan = pd.read_csv('data/grants_research_plan.csv')

# Get top 10 priority foundations
top_foundations = plan.head(10)

# Mark as researched
plan.loc[0, 'status'] = 'Completed'
plan.loc[0, 'notes'] = 'Found 15 grants on website'
plan.to_csv('data/grants_research_plan.csv', index=False)
```

## 🛠️ Dependencies

- `pandas`: Data manipulation and analysis
- `requests`: API calls and web scraping
- `openpyxl`: Excel file support
- `tqdm`: Progress bars
- `python-dotenv`: Environment variable management
- `jupyter`: Interactive data analysis

## 📝 Data Fields

### Donor-Advised Funds & Private Foundations

- `ein`: Employer Identification Number
- `name`: Organization name
- `city`, `state`: Location
- `ntee_code`: National Taxonomy classification
- `organization_type`: Classification (Donor Advised Fund, Private Foundation)
- `collection_date`: When data was collected
- `collection_timestamp`: Human-readable timestamp

### Grants Data Template

- `grantor_ein`, `grantor_name`: Foundation making the grant
- `recipient_name`, `recipient_ein`: Organization receiving grant
- `grant_amount`: Dollar amount
- `grant_purpose`: Description or purpose
- `grant_year`: Year granted
- `data_source`: Where information was found
- `source_url`: Direct link to source

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is for educational and research purposes. Please respect API rate limits and terms of service for all data sources.

## 🆘 Support

For questions or issues:

1. Check the comprehensive grants guide output
2. Review the generated research resources
3. Consult the data collection logs
4. Open an issue with detailed information

## 🎓 Educational Use

This project is ideal for:

- **Nonprofit research**: Understanding the foundation ecosystem
- **Academic studies**: Analyzing philanthropic patterns
- **Grant seeking**: Identifying potential funders
- **Data science**: Working with real-world APIs and data

---

**⚠️ Important**: This tool provides organizational data and research resources. For comprehensive grants data, consider professional databases like Candid Foundation Directory or systematic manual research using the provided guides.
