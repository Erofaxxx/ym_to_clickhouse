#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simplified program to export Yandex Metrica data to ClickHouse.
Exports only the fields used in .ipynb notebooks with parameter validation.
Creates separate tables: hits_simple and visits_simple
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
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


class YMSimpleExporter:
    """Exports Yandex Metrica data to ClickHouse with only essential fields"""

    # Fields from notebooks - HITS (8 fields)
    HITS_FIELDS = (
        'ym:pv:browser',
        'ym:pv:clientID',
        'ym:pv:date',
        'ym:pv:dateTime',
        'ym:pv:deviceCategory',
        'ym:pv:lastTrafficSource',
        'ym:pv:operatingSystemRoot',
        'ym:pv:URL'
    )

    # Fields from notebooks - VISITS (10 fields)
    VISITS_FIELDS = (
        'ym:s:browser',
        'ym:s:clientID',
        'ym:s:date',
        'ym:s:dateTime',
        'ym:s:deviceCategory',
        'ym:s:lastTrafficSource',
        'ym:s:operatingSystemRoot',
        'ym:s:purchaseID',
        'ym:s:purchaseRevenue',
        'ym:s:startURL'
    )

    def __init__(self, config):
        """
        Initialize exporter with configuration

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
                - export_hits: Export hits data (default: True)
                - export_visits: Export visits data (default: True)
        """
        self.config = config
        self.api_host = 'https://api-metrika.yandex.ru'
        self.ch_client = None

    def validate_config(self):
        """Validate required configuration parameters"""
        required_keys = [
            'ym_token', 'ym_counter_id', 'start_date', 'end_date',
            'ch_host', 'ch_user', 'ch_pass', 'ch_cacert', 'ch_database'
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
            version = self.ch_client.get_version()
            logger.info("ClickHouse connection established")
        except Exception as e:
            raise ClickHouseError(f"Failed to connect to ClickHouse: {e}")

    def validate_fields(self, source, fields):
        """
        Validate that all specified fields are available for the counter.

        Args:
            source: 'hits' or 'visits'
            fields: tuple of field names

        Returns:
            tuple: (available_fields, unavailable_fields)
        """
        logger.info(f"Validating {len(fields)} fields for {source}...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        # Test all fields together
        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', source),
            ('fields', ','.join(fields))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"

        try:
            response = requests.get(url, headers=header_dict, timeout=30)

            if response.status_code == 200:
                result = response.json().get('log_request_evaluation', {})
                if result.get('possible', False):
                    logger.info(f"✓ All {len(fields)} fields are available for {source}")
                    return list(fields), []
                else:
                    raise YandexMetricaAPIError(f"Cannot create log request for {source}")
            else:
                # Some fields might not be available
                error_data = response.json() if response.status_code == 400 else {}
                error_message = error_data.get('message', response.text)

                logger.warning(f"Field validation failed for {source}: {error_message}")

                # Try to identify problematic fields by testing individually
                available = []
                unavailable = []

                logger.info(f"Testing fields individually for {source}...")
                for field in fields:
                    url_params = urlencode([
                        ('date1', self.config['start_date']),
                        ('date2', self.config['end_date']),
                        ('source', source),
                        ('fields', field)
                    ])

                    url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests/evaluate?{url_params}"
                    response = requests.get(url, headers=header_dict, timeout=10)

                    if response.status_code == 200:
                        result = response.json().get('log_request_evaluation', {})
                        if result.get('possible', False):
                            available.append(field)
                            logger.info(f"  ✓ {field}")
                        else:
                            unavailable.append(field)
                            logger.warning(f"  ✗ {field} - not possible")
                    else:
                        unavailable.append(field)
                        logger.warning(f"  ✗ {field} - error")

                return available, unavailable

        except requests.RequestException as e:
            raise YandexMetricaAPIError(f"Failed to validate fields: {e}")

    def create_logs_request(self, source, fields):
        """Create Logs API request and return request_id"""
        logger.info(f"Creating Logs API request for {source}...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        url_params = urlencode([
            ('date1', self.config['start_date']),
            ('date2', self.config['end_date']),
            ('source', source),
            ('fields', ','.join(sorted(fields, key=lambda s: s.lower())))
        ])

        url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequests?{url_params}"

        try:
            response = requests.post(url, headers=header_dict, timeout=30)

            if response.status_code != 200:
                error_msg = f"API returned status {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f": {error_data['message']}"
                except:
                    error_msg += f". Response: {response.text[:200]}"
                raise YandexMetricaAPIError(error_msg)

            request_id = response.json()['log_request']['request_id']
            logger.info(f"✓ Logs API request created for {source} with ID: {request_id}")
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
        while status in ('created', 'processing'):
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
                    logger.info(f"✓ Request processed successfully. Parts: {len(log_request.get('parts', []))}")
                    return log_request
                elif status in ('processing_failed', 'canceled'):
                    raise YandexMetricaAPIError(f"Request processing failed with status: {status}")

            except requests.RequestException as e:
                raise YandexMetricaAPIError(f"Failed to check request status: {e}")

    def download_data(self, request_id, parts, source):
        """Download data from processed Logs API request"""
        logger.info(f"Downloading {source} data from {len(parts)} parts...")

        header_dict = {
            'Authorization': f'OAuth {self.config["ym_token"]}',
            'Content-Type': 'application/x-yametrika+json'
        }

        dataframes = []

        for part in parts:
            part_num = part['part_number']
            logger.info(f"  Downloading part {part_num}...")

            url = f"{self.api_host}/management/v1/counter/{self.config['ym_counter_id']}/logrequest/{request_id}/part/{part_num}/download"

            try:
                response = requests.get(url, headers=header_dict, timeout=300)
                response.raise_for_status()

                df = pd.read_csv(StringIO(response.text), sep='\t')
                dataframes.append(df)
                logger.info(f"  ✓ Part {part_num} downloaded: {len(df)} rows")

            except requests.RequestException as e:
                raise YandexMetricaAPIError(f"Failed to download part {part_num}: {e}")
            except Exception as e:
                raise YandexMetricaAPIError(f"Failed to parse data from part {part_num}: {e}")

        if not dataframes:
            raise YandexMetricaAPIError("No data downloaded")

        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"✓ Total rows downloaded for {source}: {len(combined_df)}")

        return combined_df

    def create_hits_table(self):
        """Create ClickHouse table for hits"""
        logger.info("Creating hits_simple table...")

        table_name = f"{self.config['ch_database']}.hits_simple"

        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Create table matching notebook schema
        create_query = f"""
        CREATE TABLE {table_name} (
            Browser String,
            ClientID UInt64,
            EventDate Date,
            EventTime DateTime,
            DeviceCategory String,
            TraficSource String,
            OSRoot String,
            URL String
        ) ENGINE = MergeTree()
        ORDER BY (intHash32(ClientID), EventDate)
        SAMPLE BY intHash32(ClientID)
        SETTINGS index_granularity=8192
        """

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"✓ Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"✓ Created table: {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to create hits table: {e}")

    def create_visits_table(self):
        """Create ClickHouse table for visits"""
        logger.info("Creating visits_simple table...")

        table_name = f"{self.config['ch_database']}.visits_simple"

        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Create table matching notebook schema
        create_query = f"""
        CREATE TABLE {table_name} (
            Browser String,
            ClientID UInt64,
            StartDate Date,
            StartTime DateTime,
            DeviceCategory String,
            TraficSource String,
            OSRoot String,
            Purchases Int32,
            Revenue Double,
            StartURL String
        ) ENGINE = MergeTree()
        ORDER BY (intHash32(ClientID), StartDate)
        SAMPLE BY intHash32(ClientID)
        SETTINGS index_granularity=8192
        """

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"✓ Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"✓ Created table: {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to create visits table: {e}")

    def upload_hits_to_clickhouse(self, df):
        """Upload hits DataFrame to ClickHouse"""
        logger.info("Uploading hits data to ClickHouse...")

        # Rename columns to match table schema (as in notebook)
        df_renamed = df.rename(columns={
            'ym:pv:browser': 'Browser',
            'ym:pv:clientID': 'ClientID',
            'ym:pv:date': 'EventDate',
            'ym:pv:dateTime': 'EventTime',
            'ym:pv:deviceCategory': 'DeviceCategory',
            'ym:pv:lastTrafficSource': 'TraficSource',
            'ym:pv:operatingSystemRoot': 'OSRoot',
            'ym:pv:URL': 'URL'
        })

        table_name = f"{self.config['ch_database']}.hits_simple"

        try:
            tsv_data = df_renamed.to_csv(sep='\t', index=False)
            self.ch_client.upload(table_name, tsv_data)
            logger.info(f"✓ Successfully uploaded {len(df)} rows to {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to upload hits data to ClickHouse: {e}")

    def upload_visits_to_clickhouse(self, df):
        """Upload visits DataFrame to ClickHouse"""
        logger.info("Uploading visits data to ClickHouse...")

        # Rename columns to match table schema (as in notebook)
        df_renamed = df.rename(columns={
            'ym:s:browser': 'Browser',
            'ym:s:clientID': 'ClientID',
            'ym:s:date': 'StartDate',
            'ym:s:dateTime': 'StartTime',
            'ym:s:deviceCategory': 'DeviceCategory',
            'ym:s:lastTrafficSource': 'TraficSource',
            'ym:s:operatingSystemRoot': 'OSRoot',
            'ym:s:startURL': 'StartURL'
        })

        # Process purchase data (as in notebook)
        df_renamed['Purchases'] = df['ym:s:purchaseRevenue'].map(
            lambda x: x.count(',') + 1 if x != '[]' else 0
        )
        df_renamed['Revenue'] = df['ym:s:purchaseRevenue'].map(
            lambda x: sum(map(float, x[1:-1].split(','))) if x != '[]' else 0
        )

        # Keep only the columns we need
        df_renamed = df_renamed[[
            'Browser', 'ClientID', 'StartDate', 'StartTime',
            'DeviceCategory', 'TraficSource', 'OSRoot',
            'Purchases', 'Revenue', 'StartURL'
        ]]

        table_name = f"{self.config['ch_database']}.visits_simple"

        try:
            tsv_data = df_renamed.to_csv(sep='\t', index=False)
            self.ch_client.upload(table_name, tsv_data)
            logger.info(f"✓ Successfully uploaded {len(df)} rows to {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to upload visits data to ClickHouse: {e}")

    def export_hits(self):
        """Export hits data"""
        logger.info("\n" + "="*60)
        logger.info("EXPORTING HITS DATA")
        logger.info("="*60)

        # Validate fields
        available_fields, unavailable_fields = self.validate_fields('hits', self.HITS_FIELDS)

        if unavailable_fields:
            logger.warning(f"⚠ Some fields are not available: {unavailable_fields}")
            logger.info(f"Continuing with {len(available_fields)} available fields")

        if not available_fields:
            raise YandexMetricaAPIError("No fields available for hits export")

        # Create request
        request_id = self.create_logs_request('hits', available_fields)

        # Wait for processing
        log_request = self.wait_for_request_processing(request_id)

        # Download data
        df = self.download_data(request_id, log_request['parts'], 'hits')

        # Create table
        self.create_hits_table()

        # Upload data
        self.upload_hits_to_clickhouse(df)

        logger.info("✓ Hits export completed successfully!\n")

    def export_visits(self):
        """Export visits data"""
        logger.info("\n" + "="*60)
        logger.info("EXPORTING VISITS DATA")
        logger.info("="*60)

        # Validate fields
        available_fields, unavailable_fields = self.validate_fields('visits', self.VISITS_FIELDS)

        if unavailable_fields:
            logger.warning(f"⚠ Some fields are not available: {unavailable_fields}")
            logger.info(f"Continuing with {len(available_fields)} available fields")

        if not available_fields:
            raise YandexMetricaAPIError("No fields available for visits export")

        # Create request
        request_id = self.create_logs_request('visits', available_fields)

        # Wait for processing
        log_request = self.wait_for_request_processing(request_id)

        # Download data
        df = self.download_data(request_id, log_request['parts'], 'visits')

        # Create table
        self.create_visits_table()

        # Upload data
        self.upload_visits_to_clickhouse(df)

        logger.info("✓ Visits export completed successfully!\n")

    def run(self):
        """Execute the export process"""
        try:
            logger.info("\n" + "="*60)
            logger.info("YANDEX METRICA SIMPLE EXPORT TO CLICKHOUSE")
            logger.info("="*60 + "\n")

            # Validate configuration
            self.validate_config()

            # Initialize ClickHouse client
            self.init_clickhouse_client()

            # Export hits if enabled
            if self.config.get('export_hits', True):
                self.export_hits()
            else:
                logger.info("Skipping hits export (disabled)")

            # Export visits if enabled
            if self.config.get('export_visits', True):
                self.export_visits()
            else:
                logger.info("Skipping visits export (disabled)")

            logger.info("="*60)
            logger.info("ALL EXPORTS COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            logger.info(f"Tables created in database '{self.config['ch_database']}':")
            if self.config.get('export_hits', True):
                logger.info("  - hits_simple")
            if self.config.get('export_visits', True):
                logger.info("  - visits_simple")

            return True

        except (YandexMetricaAPIError, ClickHouseError, ValueError) as e:
            logger.error(f"\n❌ Error during export: {e}")
            return False
        except Exception as e:
            logger.error(f"\n❌ Unexpected error: {e}", exc_info=True)
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
        'export_hits': os.getenv('EXPORT_HITS', 'true').lower() == 'true',
        'export_visits': os.getenv('EXPORT_VISITS', 'true').lower() == 'true'
    }


def main():
    parser = argparse.ArgumentParser(
        description='Simplified Yandex Metrica data export to ClickHouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script exports only the fields used in .ipynb notebooks:
  - Hits: 8 fields (browser, clientID, date, dateTime, deviceCategory,
          lastTrafficSource, operatingSystemRoot, URL)
  - Visits: 10 fields (browser, clientID, date, dateTime, deviceCategory,
            lastTrafficSource, operatingSystemRoot, purchaseID,
            purchaseRevenue, startURL)

Tables created:
  - hits_simple (instead of hits)
  - visits_simple (instead of visits)

Examples:
  # Using config file
  python export_ym_simple.py --config config.json

  # Using environment variables
  export YM_TOKEN=your_token
  export YM_COUNTER_ID=123456
  export YM_START_DATE=2024-01-01
  export YM_END_DATE=2024-01-31
  export CH_HOST=https://clickhouse.example.com:8443
  export CH_USER=user
  export CH_PASS=password
  export CH_DATABASE=analytics
  python export_ym_simple.py

  # Export only hits or only visits
  export EXPORT_HITS=true
  export EXPORT_VISITS=false
  python export_ym_simple.py --config config.json
        """
    )

    parser.add_argument(
        '--config',
        help='Path to JSON configuration file',
        default=None
    )

    parser.add_argument(
        '--hits-only',
        action='store_true',
        help='Export only hits data'
    )

    parser.add_argument(
        '--visits-only',
        action='store_true',
        help='Export only visits data'
    )

    args = parser.parse_args()

    # Load configuration
    if args.config:
        config = load_config_from_file(args.config)
    else:
        config = load_config_from_env()

    # Handle command line flags for selective export
    if args.hits_only:
        config['export_hits'] = True
        config['export_visits'] = False
    elif args.visits_only:
        config['export_hits'] = False
        config['export_visits'] = True

    # Create exporter and run
    exporter = YMSimpleExporter(config)
    success = exporter.run()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
