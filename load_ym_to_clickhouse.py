#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Program to load Yandex Metrica visits data to ClickHouse.
Supports configuration via config file or environment variables.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timedelta
from urllib.parse import urlencode
from io import StringIO

import requests
import pandas as pd

from some_funcs import simple_ch_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YandexMetricaAPIError(Exception):
    """Custom exception for Yandex Metrica API errors"""
    pass


class ClickHouseError(Exception):
    """Custom exception for ClickHouse errors"""
    pass


class YMToClickHouseLoader:
    """Loads Yandex Metrica visits data to ClickHouse"""

    # Fields to extract from Yandex Metrica API (visits)
    # Note: Some advertising fields like DirectPlatform and DirectConditionType
    # are not available for all counters and have been removed
    API_FIELDS = (
        'ym:s:visitID',
        'ym:s:watchIDs',
        'ym:s:date',
        'ym:s:isNewUser',
        'ym:s:startURL',
        'ym:s:endURL',
        'ym:s:visitDuration',
        'ym:s:bounce',
        'ym:s:clientID',
        'ym:s:goalsID',
        'ym:s:goalsDateTime',
        'ym:s:referer',
        'ym:s:deviceCategory',
        'ym:s:operatingSystemRoot',
        'ym:s:UTMCampaign',
        'ym:s:UTMContent',
        'ym:s:UTMMedium',
        'ym:s:UTMSource',
        'ym:s:UTMTerm',
        'ym:s:TrafficSource',
        'ym:s:pageViews',
        'ym:s:purchaseID',
        'ym:s:purchaseDateTime',
        'ym:s:purchaseRevenue',
        'ym:s:purchaseCurrency',
        'ym:s:purchaseProductQuantity',
        'ym:s:productsPurchaseID',
        'ym:s:productsID',
        'ym:s:productsName',
        'ym:s:productsCategory',
        'ym:s:regionCity',
        'ym:s:impressionsURL',
        'ym:s:impressionsDateTime',
        'ym:s:impressionsProductID',
        'ym:s:AdvEngine',
        'ym:s:ReferalSource',
        'ym:s:SearchEngineRoot',
        'ym:s:SearchPhrase'
    )

    def __init__(self, config):
        """
        Initialize loader with configuration

        Args:
            config (dict): Configuration dictionary with keys:
                - ym_token: Yandex Metrica OAuth token
                - ym_counter_id: Counter ID
                - start_date: Start date (YYYY-MM-DD)
                - end_date: End date (YYYY-MM-DD)
                - ch_host: ClickHouse host URL
                - ch_user: ClickHouse user
                - ch_pass: ClickHouse password
                - ch_cacert: Path to CA certificate
                - ch_database: ClickHouse database name
                - ch_table: ClickHouse table name
        """
        self.config = config
        self.api_host = 'https://api-metrika.yandex.ru'
        self.ch_client = None
        self.available_fields = None  # Will be populated after field detection

    def validate_config(self):
        """Validate required configuration parameters"""
        required_keys = [
            'ym_token', 'ym_counter_id', 'start_date', 'end_date',
            'ch_host', 'ch_user', 'ch_pass', 'ch_cacert', 'ch_database', 'ch_table'
        ]

        missing_keys = [key for key in required_keys if not self.config.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required configuration: {', '.join(missing_keys)}")

        # Validate dates
        try:
            start_date = datetime.strptime(self.config['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(self.config['end_date'], '%Y-%m-%d')
            today = datetime.now()

            if start_date > today or end_date > today:
                raise ValueError(f"Dates cannot be in the future. Today is {today.strftime('%Y-%m-%d')}")

            if start_date > end_date:
                raise ValueError(f"Start date ({self.config['start_date']}) cannot be after end date ({self.config['end_date']})")

        except ValueError as e:
            if "does not match format" in str(e):
                raise ValueError(f"Invalid date format. Use YYYY-MM-DD format. Error: {e}")
            raise

        logger.info("Configuration validated successfully")

    def init_clickhouse_client(self):
        """Initialize ClickHouse client"""
        try:
            self.ch_client = simple_ch_client(
                self.config['ch_host'],
                self.config['ch_user'],
                self.config['ch_pass'],
                self.config['ch_cacert']
            )
            # Test connection
            self.ch_client.get_version()
            logger.info("ClickHouse connection established")
        except Exception as e:
            raise ClickHouseError(f"Failed to connect to ClickHouse: {e}")

    def detect_available_fields(self):
        """
        Automatically detect which fields are available for the counter.
        Tests each field and filters out unavailable ones.
        """
        logger.info("Detecting available fields for the counter...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        # Start with minimal required fields that should always be available
        base_fields = ['ym:s:visitID', 'ym:s:date', 'ym:s:clientID']

        # Test with base fields first
        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(base_fields))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"

        try:
            response = requests.get(url, headers=header_dict, timeout=30)
            if response.status_code != 200:
                raise YandexMetricaAPIError(f"Cannot access counter API: {response.status_code}")
        except requests.RequestException as e:
            raise YandexMetricaAPIError(f"Failed to connect to API: {e}")

        # Now test all fields together and identify unavailable ones
        available = list(base_fields)  # Start with base fields
        unavailable = []

        # Test remaining fields in batches
        remaining_fields = [f for f in self.API_FIELDS if f not in base_fields]

        logger.info(f"Testing {len(remaining_fields)} additional fields...")

        # Try all fields together first
        test_fields = base_fields + remaining_fields
        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(test_fields))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"

        try:
            response = requests.get(url, headers=header_dict, timeout=30)

            if response.status_code == 200:
                # All fields are available
                available = list(self.API_FIELDS)
                logger.info(f"All {len(available)} fields are available!")
            else:
                # Some fields are not available, need to identify which ones
                logger.info("Some fields are unavailable, testing individually...")

                # Extract field name from error if possible
                error_data = response.json() if response.status_code == 400 else {}
                error_message = error_data.get('message', '')

                # Try to extract unavailable field from error message
                import re
                match = re.search(r'ym:s:\w+', error_message)
                if match:
                    unavailable_field = match.group(0)
                    logger.info(f"Identified unavailable field from error: {unavailable_field}")
                    unavailable.append(unavailable_field)

                    # Remove it and try again recursively
                    test_fields = [f for f in test_fields if f != unavailable_field]

                    # Test again with this field removed
                    while test_fields:
                        url_params = urlencode([
                            ('date1', self.config['start_date']),
                            ('date2', self.config['end_date']),
                            ('source', 'visits'),
                            ('fields', ','.join(test_fields))
                        ])

                        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"
                        response = requests.get(url, headers=header_dict, timeout=30)

                        if response.status_code == 200:
                            available = test_fields
                            break
                        else:
                            error_data = response.json() if response.status_code == 400 else {}
                            error_message = error_data.get('message', '')
                            match = re.search(r'ym:s:\w+', error_message)
                            if match:
                                unavailable_field = match.group(0)
                                if unavailable_field not in unavailable:
                                    logger.info(f"Found another unavailable field: {unavailable_field}")
                                    unavailable.append(unavailable_field)
                                    test_fields = [f for f in test_fields if f != unavailable_field]
                            else:
                                # Can't identify the problematic field, give up
                                logger.warning("Cannot identify unavailable field, using base fields only")
                                available = base_fields
                                break
                else:
                    # Fallback: test each field individually
                    logger.info("Testing fields individually (this may take a while)...")
                    for field in remaining_fields:
                        test_fields = base_fields + [field]
                        url_params = urlencode([
                            ('date1', self.config['start_date']),
                            ('date2', self.config['end_date']),
                            ('source', 'visits'),
                            ('fields', ','.join(test_fields))
                        ])

                        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"
                        response = requests.get(url, headers=header_dict, timeout=10)

                        if response.status_code == 200:
                            available.append(field)
                        else:
                            unavailable.append(field)

        except requests.RequestException as e:
            logger.warning(f"Error during field detection: {e}, using all fields")
            available = list(self.API_FIELDS)

        self.available_fields = tuple(available)

        if unavailable:
            logger.warning(f"Removed {len(unavailable)} unavailable fields: {', '.join([f.split(':')[-1] for f in unavailable])}")

        logger.info(f"Using {len(self.available_fields)} available fields")

        return self.available_fields

    def check_logs_api_availability(self):
        """Check if Logs API request can be created"""
        logger.info("Checking Logs API availability...")

        # Use detected available fields or fall back to all fields
        fields_to_use = self.available_fields if self.available_fields else self.API_FIELDS

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(fields_to_use))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"

        try:
            response = requests.get(url, headers=header_dict, timeout=30)

            # Check for errors and provide detailed error message
            if response.status_code != 200:
                error_msg = f"API returned status {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f": {error_data['message']}"
                    if 'errors' in error_data:
                        error_msg += f". Errors: {error_data['errors']}"
                except:
                    error_msg += f". Response: {response.text[:200]}"
                raise YandexMetricaAPIError(error_msg)

            result = response.json().get('log_request_evaluation', {})

            if not result.get('possible', False):
                raise YandexMetricaAPIError("Logs API request is not possible for the specified parameters")

            logger.info(f"Logs API available. Expected size: {result.get('expected_size', 0)} bytes")
            return True

        except requests.RequestException as e:
            raise YandexMetricaAPIError(f"Failed to check Logs API availability: {e}")

    def create_logs_request(self):
        """Create Logs API request and return request_id"""
        logger.info("Creating Logs API request...")

        # Use detected available fields or fall back to all fields
        fields_to_use = self.available_fields if self.available_fields else self.API_FIELDS

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(sorted(fields_to_use, key=lambda s: s.lower())))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests?{url_params}"

        try:
            response = requests.post(url, headers=header_dict, timeout=30)

            # Check for errors and provide detailed error message
            if response.status_code != 200:
                error_msg = f"API returned status {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f": {error_data['message']}"
                    if 'errors' in error_data:
                        error_msg += f". Errors: {error_data['errors']}"
                except:
                    error_msg += f". Response: {response.text[:200]}"
                raise YandexMetricaAPIError(error_msg)

            request_id = response.json()['log_request']['request_id']
            logger.info(f"Logs API request created with ID: {request_id}")
            return request_id

        except requests.RequestException as e:
            raise YandexMetricaAPIError(f"Failed to create Logs API request: {e}")

    def wait_for_request_processing(self, request_id, max_wait_minutes=30):
        """Wait for Logs API request to be processed"""
        logger.info(f"Waiting for request {request_id} to be processed...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequest/{request_id}"

        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60

        status = 'created'
        while status == 'created' or status == 'processing':
            if time.time() - start_time > max_wait_seconds:
                raise YandexMetricaAPIError(f"Request processing timeout after {max_wait_minutes} minutes")

            time.sleep(10)  # Check every 10 seconds

            try:
                response = requests.get(url, headers=header_dict, timeout=30)
                response.raise_for_status()

                log_request = response.json()['log_request']
                status = log_request['status']

                logger.info(f"Request status: {status}")

                if status == 'processed':
                    logger.info(f"Request processed successfully. Parts: {len(log_request.get('parts', []))}")
                    return log_request
                elif status == 'processing_failed' or status == 'canceled':
                    raise YandexMetricaAPIError(f"Request processing failed with status: {status}")

            except requests.RequestException as e:
                raise YandexMetricaAPIError(f"Failed to check request status: {e}")

    def download_data(self, request_id, parts):
        """Download data from processed Logs API request"""
        logger.info(f"Downloading data from {len(parts)} parts...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        dataframes = []

        for part in parts:
            part_num = part['part_number']
            logger.info(f"Downloading part {part_num}...")

            url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequest/{request_id}/part/{part_num}/download"

            try:
                response = requests.get(url, headers=header_dict, timeout=300)
                response.raise_for_status()

                df = pd.read_csv(StringIO(response.text), sep='\t')
                dataframes.append(df)
                logger.info(f"Part {part_num} downloaded: {len(df)} rows")

            except requests.RequestException as e:
                raise YandexMetricaAPIError(f"Failed to download part {part_num}: {e}")
            except Exception as e:
                raise YandexMetricaAPIError(f"Failed to parse data from part {part_num}: {e}")

        if not dataframes:
            raise YandexMetricaAPIError("No data downloaded")

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Total rows downloaded: {len(combined_df)}")

        return combined_df

    def create_clickhouse_table(self):
        """Create or recreate ClickHouse table dynamically based on available fields"""
        logger.info("Creating ClickHouse table...")

        table_name = f"{self.config['ch_database']}.{self.config['ch_table']}"
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Use available fields to build dynamic schema
        fields_to_use = self.available_fields if self.available_fields else self.API_FIELDS

        # Define column types for each field
        field_types = {
            'ym:s:visitID': 'visitID UInt64',
            'ym:s:watchIDs': 'watchIDs String',
            'ym:s:date': 'date Date',
            'ym:s:isNewUser': 'isNewUser UInt8',
            'ym:s:startURL': 'startURL String',
            'ym:s:endURL': 'endURL String',
            'ym:s:visitDuration': 'visitDuration UInt32',
            'ym:s:bounce': 'bounce UInt8',
            'ym:s:clientID': 'clientID UInt64',
            'ym:s:goalsID': 'goalsID String',
            'ym:s:goalsDateTime': 'goalsDateTime String',
            'ym:s:referer': 'referer String',
            'ym:s:deviceCategory': 'deviceCategory String',
            'ym:s:operatingSystemRoot': 'operatingSystemRoot String',
            'ym:s:UTMCampaign': 'UTMCampaign String',
            'ym:s:UTMContent': 'UTMContent String',
            'ym:s:UTMMedium': 'UTMMedium String',
            'ym:s:UTMSource': 'UTMSource String',
            'ym:s:UTMTerm': 'UTMTerm String',
            'ym:s:TrafficSource': 'TrafficSource String',
            'ym:s:pageViews': 'pageViews UInt32',
            'ym:s:purchaseID': 'purchaseID String',
            'ym:s:purchaseDateTime': 'purchaseDateTime String',
            'ym:s:purchaseRevenue': 'purchaseRevenue String',
            'ym:s:purchaseCurrency': 'purchaseCurrency String',
            'ym:s:purchaseProductQuantity': 'purchaseProductQuantity String',
            'ym:s:productsPurchaseID': 'productsPurchaseID String',
            'ym:s:productsID': 'productsID String',
            'ym:s:productsName': 'productsName String',
            'ym:s:productsCategory': 'productsCategory String',
            'ym:s:regionCity': 'regionCity String',
            'ym:s:impressionsURL': 'impressionsURL String',
            'ym:s:impressionsDateTime': 'impressionsDateTime String',
            'ym:s:impressionsProductID': 'impressionsProductID String',
            'ym:s:AdvEngine': 'AdvEngine String',
            'ym:s:ReferalSource': 'ReferalSource String',
            'ym:s:SearchEngineRoot': 'SearchEngineRoot String',
            'ym:s:SearchPhrase': 'SearchPhrase String'
        }

        # Build column definitions for available fields
        column_defs = []
        for field in fields_to_use:
            if field in field_types:
                column_defs.append(f"            {field_types[field]}")

        columns_str = ',\n'.join(column_defs)

        # Define table schema based on available fields
        create_query = f"""
        CREATE TABLE {table_name} (
{columns_str}
        ) ENGINE = MergeTree()
        ORDER BY (clientID, date)
        SETTINGS index_granularity=8192
        """

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"Created table: {table_name} with {len(column_defs)} columns")

        except Exception as e:
            raise ClickHouseError(f"Failed to create table: {e}")

    def upload_to_clickhouse(self, df):
        """Upload DataFrame to ClickHouse"""
        logger.info("Uploading data to ClickHouse...")

        # Rename columns to match table schema (remove 'ym:s:' prefix)
        df_renamed = df.copy()
        df_renamed.columns = [col.replace('ym:s:', '') for col in df.columns]

        table_name = f"{self.config['ch_database']}.{self.config['ch_table']}"

        try:
            # Convert DataFrame to TSV format
            tsv_data = df_renamed.to_csv(sep='\t', index=False)

            # Upload to ClickHouse
            self.ch_client.upload(table_name, tsv_data)
            logger.info(f"Successfully uploaded {len(df)} rows to {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to upload data to ClickHouse: {e}")

    def run(self):
        """Execute the full ETL process"""
        try:
            logger.info("Starting Yandex Metrica to ClickHouse data load...")

            # Validate configuration
            self.validate_config()

            # Initialize ClickHouse client
            self.init_clickhouse_client()

            # Detect available fields for this counter
            self.detect_available_fields()

            # Check Logs API availability
            self.check_logs_api_availability()

            # Create Logs API request
            request_id = self.create_logs_request()

            # Wait for processing
            log_request = self.wait_for_request_processing(request_id)

            # Download data
            df = self.download_data(request_id, log_request['parts'])

            # Create ClickHouse table
            self.create_clickhouse_table()

            # Upload to ClickHouse
            self.upload_to_clickhouse(df)

            logger.info("Data load completed successfully!")
            return True

        except (YandexMetricaAPIError, ClickHouseError, ValueError) as e:
            logger.error(f"Error during data load: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False


def load_config_from_file(config_path):
    """Load configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        sys.exit(1)


def load_config_from_env():
    """Load configuration from environment variables"""
    return {
        'ym_token': os.getenv('YM_TOKEN'),
        'ym_counter_id': os.getenv('YM_COUNTER_ID'),
        'start_date': os.getenv('YM_START_DATE'),
        'end_date': os.getenv('YM_END_DATE'),
        'ch_host': os.getenv('CH_HOST'),
        'ch_user': os.getenv('CH_USER'),
        'ch_pass': os.getenv('CH_PASS'),
        'ch_cacert': os.getenv('CH_CACERT', 'YandexInternalRootCA.crt'),
        'ch_database': os.getenv('CH_DATABASE', 'default'),
        'ch_table': os.getenv('CH_TABLE', 'ym_visits')
    }


def main():
    parser = argparse.ArgumentParser(
        description='Load Yandex Metrica visits data to ClickHouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using config file
  python load_ym_to_clickhouse.py --config config.json

  # Using environment variables
  export YM_TOKEN=your_token
  export YM_COUNTER_ID=123456
  export YM_START_DATE=2024-01-01
  export YM_END_DATE=2024-01-31
  export CH_HOST=https://clickhouse.example.com:8443
  export CH_USER=user
  export CH_PASS=password
  export CH_DATABASE=analytics
  export CH_TABLE=ym_visits
  python load_ym_to_clickhouse.py
        """
    )

    parser.add_argument(
        '--config',
        help='Path to JSON configuration file',
        default=None
    )

    args = parser.parse_args()

    # Load configuration
    if args.config:
        config = load_config_from_file(args.config)
    else:
        config = load_config_from_env()

    # Create loader and run
    loader = YMToClickHouseLoader(config)
    success = loader.run()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
