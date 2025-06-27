# ProPublica IRS 990 Data Analysis for Donor Research

A streamlined Python workflow to identify potential funders for nonprofits by analyzing IRS 990 filing data from ProPublica's Nonprofit Explorer API.

## 🎯 Project Overview

This project helps nonprofits find potential funders by analyzing:

- **Donor-Advised Funds**: Charitable giving accounts that actively seek worthy causes
- **Private Foundations (990-PF filers)**: Organizations required to distribute 5% of assets annually
- **Grant Patterns**: Historical giving data to understand funder preferences

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Get ProPublica API key (free)
# Visit: https://projects.propublica.org/nonprofits/api
# Add to .env file: PROPUBLICA_API_KEY=your_key_here
```

### 2. Collect Data

```bash
# Run the data collection script (may take 2-4 hours for complete data)
python collect_data.py
```

### 3. Analyze Data

```bash
# Open the analysis notebook
jupyter notebook analyze_data.ipynb
```

## 📁 Simple Project Structure

```
project-donors/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .env                        # API key (create this)
├── collect_data.py             # Data collection script
├── analyze_data.ipynb          # Analysis notebook
└── data/                       # Generated datasets
    ├── donor_advised_funds_*.csv
    ├── private_foundations_*.csv
    ├── grants_data_*.csv
    ├── prospect_list_*.csv
    └── analysis_summary_*.json
```

## 🛠️ Two-File Workflow

### 1. `collect_data.py` - Data Collection

- **Complete Data Collector**: Pulls ALL available organizations and filings
- **Multiple Datasets**: Saves donor-advised funds, private foundations, and grants data
- **Smart Search**: Uses multiple search terms and filters
- **Rate Limited**: Respects API limits with built-in delays
- **Progress Tracking**: Shows real-time progress and logging

**What it does:**

```python
python collect_data.py
# → Searches for donor-advised funds
# → Searches for private foundations
# → Extracts ALL grant records from filings
# → Saves multiple CSV/Excel files to /data
# → Creates summary statistics
```

### 2. `analyze_data.ipynb` - Analysis Notebook

- **Interactive Analysis**: Step-by-step Jupyter notebook
- **Customizable Focus Areas**: Education, health, arts, environment, etc.
- **Funder Matching**: Finds funders based on your keywords
- **Visualizations**: Charts and graphs to understand the data
- **Export Tools**: Creates prospect lists and reports

**What it does:**

- Loads your collected data
- Analyzes grant patterns
- Finds funders matching your cause area
- Creates visualizations
- Exports prospect lists and reports

## 📊 What You'll Get

### Data Files (CSV & Excel)

- **Donor-Advised Funds**: Complete list with contact information
- **Private Foundations**: Foundations required to make grants
- **Grants Data**: Historical grant records with amounts and recipients
- **Prospect Lists**: Ranked potential funders for your cause area
- **Analysis Reports**: Detailed Excel workbooks with multiple sheets

### Analysis Results

- Geographic distribution of funders
- Grant size distributions and patterns
- Targeted funder lists based on your keywords
- Detailed grant-making patterns for top prospects
- Comprehensive visualizations and charts

## 🎯 Customization for Your Nonprofit

The analysis notebook includes pre-configured focus areas:

### Built-in Focus Areas

- **Education**: Schools, STEM, scholarships, literacy
- **Health**: Medical research, healthcare, mental health
- **Arts**: Culture, museums, performance, visual arts
- **Environment**: Conservation, climate, sustainability
- **Social Services**: Community, housing, food security

### Custom Configuration

```python
# In the notebook, easily customize for your organization:
CHOSEN_FOCUS = 'education'  # Change this
CUSTOM_KEYWORDS = ['your', 'specific', 'keywords']  # Add these
```

## 🔧 API Configuration

### Get Your ProPublica API Key

1. Visit: https://projects.propublica.org/nonprofits/api
2. Sign up for a free account
3. Add your key to `.env` file:
   ```
   PROPUBLICA_API_KEY=your_key_here
   ```

### Data Collection Scale

- **Unlimited by Default**: Pulls ALL available organizations and filings
- **Large Datasets**: May result in thousands of organizations
- **Processing Time**: 2-4 hours for complete collection
- **Rate Limited**: Respects API limits automatically

## 📋 Usage Example

```bash
# 1. Collect all data (run once, takes time)
python collect_data.py

# 2. Analyze for your focus area (interactive)
jupyter notebook analyze_data.ipynb

# 3. Get your results
# → data/prospect_list_education_20250626_143022.csv
# → data/targeted_funders_education_20250626_143022.csv
# → data/comprehensive_analysis_education_20250626_143022.xlsx
```

## ⚠️ Important Considerations

### Data Collection Scale

- **Complete Data**: The script pulls ALL available organizations and filings
- **Large Processing Time**: May take 2-4 hours for full collection
- **Substantial Data**: Expect thousands of organizations and grants
- **API Rate Limits**: Built-in delays respect ProPublica's limits

### Data Quality

- ProPublica data comes from IRS filings with varying quality
- Not all grants are reported in searchable formats
- Recent filings may not be immediately available

### Best Practices

- Run data collection overnight or during off-hours
- Start with the analysis notebook to understand the data structure
- Customize keywords to match your specific mission
- Focus on local funders when possible

## 🆘 Troubleshooting

### Common Issues

**No API Key Error**

```
Solution: Add PROPUBLICA_API_KEY=your_key to .env file
```

**No Data Files Found**

```
Solution: Run 'python collect_data.py' first
```

**Long Processing Time**

```
Expected: 2-4 hours for complete data collection
Monitor: Check console output for progress
```

**Large File Sizes**

```
Normal: Excel files may be 10+ MB with thousands of records
Solution: Use CSV files for faster loading if needed
```

## 📊 Sample Output

After running the complete workflow, you'll have:

### Prospect Lists

- **Complete List**: All organizations ranked by size/revenue
- **Targeted List**: Funders with history in your focus area
- **Contact Info**: Names, locations, EINs for research

### Analysis Reports

- **Excel Workbook**: Multiple sheets with different analyses
- **Visualizations**: Charts showing funder patterns
- **Summary Statistics**: Key metrics and totals

### Research Starting Points

- Organizations to research further
- Grant patterns to understand
- Contact information for outreach

## 🎉 Success Tips

### Research Phase

1. **Start Local**: Focus on funders in your geographic area
2. **Match Mission**: Look for funders with similar cause focus
3. **Check Recent Grants**: Verify they're still actively giving
4. **Understand Patterns**: Note their typical grant sizes and frequency

### Outreach Phase

1. **Follow Guidelines**: Respect each funder's application process
2. **Build Relationships**: Connect before asking for money
3. **Show Impact**: Demonstrate clear outcomes and metrics
4. **Be Patient**: Relationship building takes time

---

## 🤝 Contributing

This is a simplified, two-file workflow. If you'd like to add features:

1. Enhance the data collection script
2. Add analysis functions to the notebook
3. Submit pull requests with improvements

## 📜 License

MIT License - feel free to use and modify for your nonprofit's needs.

## 🙏 Acknowledgments

- **ProPublica** for providing free access to IRS 990 data
- **IRS** for making nonprofit data publicly available
- The nonprofit community for inspiring this work

---

**Ready to find your next funder?** Start with `python collect_data.py` and then open the analysis notebook!
