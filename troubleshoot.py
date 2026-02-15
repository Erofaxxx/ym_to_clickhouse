#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Troubleshooting script to diagnose Yandex Metrica API issues.
This script helps identify common problems before running the full load script.
"""

import json
import sys
import argparse
from datetime import datetime
import requests

def test_api_access(config):
    """Test basic API access with minimal request"""
    print("\n=== Testing Yandex Metrica API Access ===\n")

    # Test 1: Check token format
    print("1. Checking token format...")
    token = config.get('ym_token', '')
    if not token:
        print("   ❌ Error: Token is empty")
        return False
    if len(token) < 20:
        print("   ⚠️  Warning: Token seems too short (might be invalid)")
    print(f"   ✓ Token present (length: {len(token)})")

    # Test 2: Check counter access
    print("\n2. Testing counter access...")
    counter_id = config.get('ym_counter_id')
    api_host = 'https://api-metrika.yandex.ru'

    headers = {
        'Authorization': f'OAuth {token}',
        'Content-Type': 'application/x-yametrika+json'
    }

    try:
        url = f"{api_host}/management/v1/counter/{counter_id}"
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            counter_info = response.json()
            print(f"   ✓ Counter found: {counter_info.get('counter', {}).get('name', 'N/A')}")
        elif response.status_code == 403:
            print("   ❌ Error: Access denied. Check your token permissions.")
            return False
        elif response.status_code == 404:
            print(f"   ❌ Error: Counter {counter_id} not found.")
            return False
        else:
            print(f"   ❌ Error: API returned status {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Details: {error_data.get('message', response.text[:200])}")
            except:
                print(f"   Response: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

    # Test 3: Check dates
    print("\n3. Checking dates...")
    start_date = config.get('start_date')
    end_date = config.get('end_date')

    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        today = datetime.now()

        print(f"   Start date: {start_date}")
        print(f"   End date: {end_date}")
        print(f"   Today: {today.strftime('%Y-%m-%d')}")

        if start > today or end > today:
            print("   ⚠️  Warning: Dates are in the future!")
            return False

        if start > end:
            print("   ❌ Error: Start date is after end date")
            return False

        days = (end - start).days
        print(f"   ✓ Date range: {days} days")

        if days > 90:
            print("   ⚠️  Warning: Large date range (>90 days) may cause issues")
    except ValueError as e:
        print(f"   ❌ Error: Invalid date format: {e}")
        return False

    # Test 4: Test simple Logs API request
    print("\n4. Testing Logs API with minimal fields...")

    # Try with just a few basic fields first
    minimal_fields = [
        'ym:s:visitID',
        'ym:s:date',
        'ym:s:clientID'
    ]

    from urllib.parse import urlencode

    url_params = urlencode([
        ('date1', start_date),
        ('date2', end_date),
        ('source', 'visits'),
        ('fields', ','.join(minimal_fields))
    ])

    url = f"{api_host}/management/v1/counter/{counter_id}/logrequests/evaluate?{url_params}"

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json().get('log_request_evaluation', {})
            if result.get('possible'):
                print(f"   ✓ Logs API is accessible")
                print(f"   Expected data size: {result.get('expected_size', 0)} bytes")
            else:
                print("   ❌ Error: Logs API reports data is not available")
                return False
        else:
            print(f"   ❌ Error: API returned status {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Message: {error_data.get('message', 'N/A')}")
                if 'errors' in error_data:
                    print(f"   Errors: {error_data['errors']}")
            except:
                print(f"   Response: {response.text[:300]}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

    # Test 5: Test with all fields
    print("\n5. Testing Logs API with all fields...")

    # Note: DirectPlatform and DirectConditionType removed as they're not available for all counters
    all_fields = [
        'ym:s:visitID', 'ym:s:watchIDs', 'ym:s:date', 'ym:s:isNewUser',
        'ym:s:startURL', 'ym:s:endURL', 'ym:s:visitDuration', 'ym:s:bounce',
        'ym:s:clientID', 'ym:s:goalsID', 'ym:s:goalsDateTime', 'ym:s:referer',
        'ym:s:deviceCategory', 'ym:s:operatingSystemRoot',
        'ym:s:UTMCampaign', 'ym:s:UTMContent',
        'ym:s:UTMMedium', 'ym:s:UTMSource', 'ym:s:UTMTerm', 'ym:s:TrafficSource',
        'ym:s:pageViews', 'ym:s:purchaseID', 'ym:s:purchaseDateTime',
        'ym:s:purchaseRevenue', 'ym:s:purchaseCurrency', 'ym:s:purchaseProductQuantity',
        'ym:s:productsPurchaseID', 'ym:s:productsID', 'ym:s:productsName',
        'ym:s:productsCategory', 'ym:s:regionCity', 'ym:s:impressionsURL',
        'ym:s:impressionsDateTime', 'ym:s:impressionsProductID', 'ym:s:AdvEngine',
        'ym:s:ReferalSource', 'ym:s:SearchEngineRoot', 'ym:s:SearchPhrase'
    ]

    url_params = urlencode([
        ('date1', start_date),
        ('date2', end_date),
        ('source', 'visits'),
        ('fields', ','.join(all_fields))
    ])

    url = f"{api_host}/management/v1/counter/{counter_id}/logrequests/evaluate?{url_params}"

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json().get('log_request_evaluation', {})
            if result.get('possible'):
                print(f"   ✓ All fields are available!")
                print(f"   Expected data size: {result.get('expected_size', 0)} bytes")
            else:
                print("   ❌ Error: Some fields are not available for this counter")
                print("   Suggestion: Try using fewer fields or check which fields are supported")
                return False
        else:
            print(f"   ❌ Error: API returned status {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Message: {error_data.get('message', 'N/A')}")
                if 'errors' in error_data:
                    print(f"   Errors: {error_data['errors']}")
                    print("\n   💡 Suggestion: Some e-commerce or advertising fields might not be")
                    print("      available if those features are not used in your counter.")
                    print("      Consider removing fields you don't need.")
            except:
                print(f"   Response: {response.text[:300]}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

    print("\n" + "="*50)
    print("✓ All tests passed! You should be able to load data.")
    print("="*50)
    return True

def main():
    parser = argparse.ArgumentParser(
        description='Troubleshoot Yandex Metrica API connection issues'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to config.json file'
    )

    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        sys.exit(1)

    success = test_api_access(config)
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
