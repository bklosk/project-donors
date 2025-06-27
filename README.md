# 📊 Project Donors: Nonprofit Data Collection

Simple tool to collect and analyze donor-advised funds, private foundations, and grants data from public sources for grant research.

## 🎯 What It Does

• **Collects** donor-advised funds and private foundation data from ProPublica API
• **Generates** research resources and prioritized foundation lists  
• **Analyzes** data with geographic and financial insights
• **Exports** targeted datasets for grant seeking

## 📁 Files

```
project-donors/
├── data_collector.py     # Main collection script
├── analyze_data.ipynb    # Analysis notebook
└── data/                 # Generated data files
```

## 🚀 Quick Start

```bash
# Clone or download the project
cd project-donors

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run Data Collection

```bash
# Basic organizational data collection
python data_collector.py

# Include comprehensive grants research tools
python data_collector.py --include-grants

# Skip the research guide output
python data_collector.py --skip-guide
```

### Analyze Data

```bash
# Open Jupyter notebook for analysis
jupyter notebook analyze_data.ipynb
```

## 📊 What You Get

• **Organizational Data**:

- Donor-advised funds and private foundations from ProPublica API
- Financial data, locations, classifications
- Consolidated CSV/Excel files with timestamps

• **Research Resources**:

- Prioritized foundation research plan with URLs
- Collection template for recording grants
- Comprehensive research guide

• **Analysis & Insights**:

- Geographic distribution maps
- Financial analysis and trends
- Research recommendations
- Targeted export files

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

---

**⚠️ Important**: This tool provides organizational data and research resources. For comprehensive grants data, consider professional databases like Candid Foundation Directory or systematic manual research using the provided guides.
