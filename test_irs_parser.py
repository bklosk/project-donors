#!/usr/bin/env python3
"""
Test script for IRS 990 parser
"""

from irs_990_parser import IRS990Parser
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def test_parser():
    parser = IRS990Parser()
    
    # Test with Crown Family Foundation (known to have grants)
    ein = '36-3330462'
    print(f'Testing IRS 990 parser with EIN {ein} (Crown Family Foundation)...')
    
    try:
        grants = parser.collect_grants_for_organization(ein, 'Crown Family Foundation')
        print(f'Found {len(grants)} grants')
        
        if grants:
            for i, grant in enumerate(grants[:3]):  # Show first 3
                print(f'Grant {i+1}: {grant.get("recipient_name", "Unknown")} - ${grant.get("amount", 0):,}')
                print(f'  Purpose: {grant.get("purpose", "Not specified")}')
                print(f'  Year: {grant.get("filing_year", "Unknown")}')
                print()
        else:
            print("No grants found - this could mean:")
            print("  1. The organization has no grants")
            print("  2. IRS 990 files are not available in expected locations")
            print("  3. The organization files forms that don't include Schedule I")
            
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_parser()
