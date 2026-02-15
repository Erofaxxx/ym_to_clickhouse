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
        'ym:s:DirectPlatform',
        'ym:s:DirectConditionType',
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

    def validate_config(self):
        """Validate required configuration parameters"""
        required_keys = [
            'ym_token', 'ym_counter_id', 'start_date', 'end_date',
            'ch_host', 'ch_user', 'ch_pass', 'ch_cacert', 'ch_database', 'ch_table'
        ]

        missing_keys = [key for key in required_keys if not self.config.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required configuration: {', '.join(missing_keys)}")

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

    def check_logs_api_availability(self):
        """Check if Logs API request can be created"""
        logger.info("Checking Logs API availability...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(self.API_FIELDS))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"

        try:
            response = requests.get(url, headers=header_dict, timeout=30)
            response.raise_for_status()

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

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', 'visits'),
            ('fields', ','.join(sorted(self.API_FIELDS, key=lambda s: s.lower())))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests?{url_params}"

        try:
            response = requests.post(url, headers=header_dict, timeout=30)
            response.raise_for_status()

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
        """Create or recreate ClickHouse table"""
        logger.info("Creating ClickHouse table...")

        table_name = f"{self.config['ch_database']}.{self.config['ch_table']}"

        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Define table schema based on API fields
        create_query = f"""
        CREATE TABLE {table_name} (
            visitID UInt64,
            watchIDs String,
            date Date,
            isNewUser UInt8,
            startURL String,
            endURL String,
            visitDuration UInt32,
            bounce UInt8,
            clientID UInt64,
            goalsID String,
            goalsDateTime String,
            referer String,
            deviceCategory String,
            operatingSystemRoot String,
            DirectPlatform String,
            DirectConditionType String,
            UTMCampaign String,
            UTMContent String,
            UTMMedium String,
            UTMSource String,
            UTMTerm String,
            TrafficSource String,
            pageViews UInt32,
            purchaseID String,
            purchaseDateTime String,
            purchaseRevenue String,
            purchaseCurrency String,
            purchaseProductQuantity String,
            productsPurchaseID String,
            productsID String,
            productsName String,
            productsCategory String,
            regionCity String,
            impressionsURL String,
            impressionsDateTime String,
            impressionsProductID String,
            AdvEngine String,
            ReferalSource String,
            SearchEngineRoot String,
            SearchPhrase String
        ) ENGINE = MergeTree()
        ORDER BY (clientID, date)
        SETTINGS index_granularity=8192
        """

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"Created table: {table_name}")

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
