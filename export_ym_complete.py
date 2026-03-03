#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete Yandex Metrica export to ClickHouse with all available fields.
Based on export_ym_simple.py with proper field handling and validation.
Exports all ~40 parameters with graceful handling of unavailable fields.
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


class YMCompleteExporter:
    """Exports Yandex Metrica data to ClickHouse with all available fields"""

    # Complete list of fields - HITS (8 fields from notebooks)
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

    # Complete list of fields - VISITS (all ~40 fields)
    VISITS_FIELDS = (
        'ym:s:visitID',
        'ym:s:watchIDs',
        'ym:s:date',
        'ym:s:dateTime',
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
        'ym:s:browser',
        'ym:s:lastTrafficSource',
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

    # Explicit field mapping for hits (API field -> ClickHouse column)
    HITS_FIELD_MAPPING = {
        'ym:pv:browser': 'Browser',
        'ym:pv:clientID': 'ClientID',
        'ym:pv:date': 'EventDate',
        'ym:pv:dateTime': 'EventTime',
        'ym:pv:deviceCategory': 'DeviceCategory',
        'ym:pv:lastTrafficSource': 'TraficSource',
        'ym:pv:operatingSystemRoot': 'OSRoot',
        'ym:pv:URL': 'URL'
    }

    # Explicit field mapping for visits (API field -> ClickHouse column)
    VISITS_FIELD_MAPPING = {
        'ym:s:visitID': 'VisitID',
        'ym:s:watchIDs': 'WatchIDs',
        'ym:s:date': 'StartDate',
        'ym:s:dateTime': 'StartTime',
        'ym:s:isNewUser': 'IsNewUser',
        'ym:s:startURL': 'StartURL',
        'ym:s:endURL': 'EndURL',
        'ym:s:visitDuration': 'VisitDuration',
        'ym:s:bounce': 'Bounce',
        'ym:s:clientID': 'ClientID',
        'ym:s:goalsID': 'GoalsID',
        'ym:s:goalsDateTime': 'GoalsDateTime',
        'ym:s:referer': 'Referer',
        'ym:s:deviceCategory': 'DeviceCategory',
        'ym:s:operatingSystemRoot': 'OSRoot',
        'ym:s:browser': 'Browser',
        'ym:s:lastTrafficSource': 'TraficSource',
        'ym:s:UTMCampaign': 'UTMCampaign',
        'ym:s:UTMContent': 'UTMContent',
        'ym:s:UTMMedium': 'UTMMedium',
        'ym:s:UTMSource': 'UTMSource',
        'ym:s:UTMTerm': 'UTMTerm',
        'ym:s:TrafficSource': 'TrafficSource',
        'ym:s:pageViews': 'PageViews',
        'ym:s:purchaseID': 'PurchaseID',
        'ym:s:purchaseDateTime': 'PurchaseDateTime',
        'ym:s:purchaseRevenue': 'PurchaseRevenue',
        'ym:s:purchaseCurrency': 'PurchaseCurrency',
        'ym:s:purchaseProductQuantity': 'PurchaseProductQuantity',
        'ym:s:productsPurchaseID': 'ProductsPurchaseID',
        'ym:s:productsID': 'ProductsID',
        'ym:s:productsName': 'ProductsName',
        'ym:s:productsCategory': 'ProductsCategory',
        'ym:s:regionCity': 'RegionCity',
        'ym:s:impressionsURL': 'ImpressionsURL',
        'ym:s:impressionsDateTime': 'ImpressionsDateTime',
        'ym:s:impressionsProductID': 'ImpressionsProductID',
        'ym:s:AdvEngine': 'AdvEngine',
        'ym:s:ReferalSource': 'ReferalSource',
        'ym:s:SearchEngineRoot': 'SearchEngineRoot',
        'ym:s:SearchPhrase': 'SearchPhrase'
    }

    # ClickHouse column types for hits
    HITS_COLUMN_TYPES = {
        'Browser': 'String',
        'ClientID': 'UInt64',
        'EventDate': 'Date',
        'EventTime': 'DateTime',
        'DeviceCategory': 'String',
        'TraficSource': 'String',
        'OSRoot': 'String',
        'URL': 'String'
    }

    # ClickHouse column types for visits
    VISITS_COLUMN_TYPES = {
        'VisitID': 'UInt64',
        'WatchIDs': 'String',
        'StartDate': 'Date',
        'StartTime': 'DateTime',
        'IsNewUser': 'UInt8',
        'StartURL': 'String',
        'EndURL': 'String',
        'VisitDuration': 'UInt32',
        'Bounce': 'UInt8',
        'ClientID': 'UInt64',
        'GoalsID': 'String',
        'GoalsDateTime': 'String',
        'Referer': 'String',
        'DeviceCategory': 'String',
        'OSRoot': 'String',
        'Browser': 'String',
        'TraficSource': 'String',
        'UTMCampaign': 'String',
        'UTMContent': 'String',
        'UTMMedium': 'String',
        'UTMSource': 'String',
        'UTMTerm': 'String',
        'TrafficSource': 'String',
        'PageViews': 'UInt32',
        'PurchaseID': 'String',
        'PurchaseDateTime': 'String',
        'PurchaseRevenue': 'String',
        'PurchaseCurrency': 'String',
        'PurchaseProductQuantity': 'String',
        'ProductsPurchaseID': 'String',
        'ProductsID': 'String',
        'ProductsName': 'String',
        'ProductsCategory': 'String',
        'RegionCity': 'String',
        'ImpressionsURL': 'String',
        'ImpressionsDateTime': 'String',
        'ImpressionsProductID': 'String',
        'AdvEngine': 'String',
        'ReferalSource': 'String',
        'SearchEngineRoot': 'String',
        'SearchPhrase': 'String'
    }

    def __init__(self, config):
        """
        Initialize exporter with configuration

        Args:
            config (dict): Configuration dictionary
        """
        self.config = config
        self.api_host = 'https://api-metrika.yandex.ru'
        self.ch_client = None
        self.available_hits_fields = []
        self.available_visits_fields = []

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

        # Test all fields together first
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
                    logger.warning(f"Cannot create log request for {source}, testing individually...")
            else:
                logger.warning(f"Field validation failed for {source}, testing individually...")

            # Test fields individually
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

    def create_hits_table(self, available_fields):
        """Create ClickHouse table for hits based on available fields"""
        logger.info("Creating hits_complete table...")

        table_name = f"{self.config['ch_database']}.hits_complete"

        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Build column definitions only for available fields
        column_defs = []
        for api_field in available_fields:
            ch_column = self.HITS_FIELD_MAPPING.get(api_field)
            if ch_column and ch_column in self.HITS_COLUMN_TYPES:
                column_type = self.HITS_COLUMN_TYPES[ch_column]
                column_defs.append(f"    {ch_column} {column_type}")

        columns_str = ',\n'.join(column_defs)

        # Create table
        create_query = f"""
CREATE TABLE {table_name} (
{columns_str}
) ENGINE = MergeTree()
ORDER BY (intHash32(ClientID), EventDate)
SAMPLE BY intHash32(ClientID)
SETTINGS index_granularity=8192
"""

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"✓ Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"✓ Created table: {table_name} with {len(column_defs)} columns")

        except Exception as e:
            raise ClickHouseError(f"Failed to create hits table: {e}")

    def create_visits_table(self, available_fields):
        """Create ClickHouse table for visits based on available fields"""
        logger.info("Creating visits_complete table...")

        table_name = f"{self.config['ch_database']}.visits_complete"

        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Build column definitions only for available fields
        column_defs = []
        for api_field in available_fields:
            ch_column = self.VISITS_FIELD_MAPPING.get(api_field)
            if ch_column and ch_column in self.VISITS_COLUMN_TYPES:
                column_type = self.VISITS_COLUMN_TYPES[ch_column]
                column_defs.append(f"    {ch_column} {column_type}")

        columns_str = ',\n'.join(column_defs)

        # Create table
        create_query = f"""
CREATE TABLE {table_name} (
{columns_str}
) ENGINE = MergeTree()
ORDER BY (intHash32(ClientID), StartDate)
SAMPLE BY intHash32(ClientID)
SETTINGS index_granularity=8192
"""

        try:
            self.ch_client.get_clickhouse_data(drop_query)
            logger.info(f"✓ Dropped existing table (if existed): {table_name}")

            self.ch_client.get_clickhouse_data(create_query)
            logger.info(f"✓ Created table: {table_name} with {len(column_defs)} columns")

        except Exception as e:
            raise ClickHouseError(f"Failed to create visits table: {e}")

    def upload_hits_to_clickhouse(self, df, available_fields):
        """Upload hits DataFrame to ClickHouse with explicit mapping"""
        logger.info("Uploading hits data to ClickHouse...")

        # Build rename mapping only for available fields
        rename_mapping = {}
        for api_field in available_fields:
            if api_field in self.HITS_FIELD_MAPPING:
                rename_mapping[api_field] = self.HITS_FIELD_MAPPING[api_field]

        # Rename columns using explicit mapping
        df_renamed = df.rename(columns=rename_mapping)

        # Keep only the columns we need
        columns_to_keep = [self.HITS_FIELD_MAPPING[f] for f in available_fields if f in self.HITS_FIELD_MAPPING]
        df_renamed = df_renamed[columns_to_keep]

        table_name = f"{self.config['ch_database']}.hits_complete"

        try:
            tsv_data = df_renamed.to_csv(sep='\t', index=False)
            self.ch_client.upload(table_name, tsv_data)
            logger.info(f"✓ Successfully uploaded {len(df)} rows to {table_name}")

        except Exception as e:
            raise ClickHouseError(f"Failed to upload hits data to ClickHouse: {e}")

    def upload_visits_to_clickhouse(self, df, available_fields):
        """Upload visits DataFrame to ClickHouse with explicit mapping"""
        logger.info("Uploading visits data to ClickHouse...")

        # Build rename mapping only for available fields
        rename_mapping = {}
        for api_field in available_fields:
            if api_field in self.VISITS_FIELD_MAPPING:
                rename_mapping[api_field] = self.VISITS_FIELD_MAPPING[api_field]

        # Rename columns using explicit mapping
        df_renamed = df.rename(columns=rename_mapping)

        # Keep only the columns we need
        columns_to_keep = [self.VISITS_FIELD_MAPPING[f] for f in available_fields if f in self.VISITS_FIELD_MAPPING]
        df_renamed = df_renamed[columns_to_keep]

        table_name = f"{self.config['ch_database']}.visits_complete"

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
            logger.warning(f"⚠ Some fields are not available and will be skipped:")
            for field in unavailable_fields:
                logger.warning(f"  - {field}")
            logger.info(f"Continuing with {len(available_fields)} available fields")

        if not available_fields:
            raise YandexMetricaAPIError("No fields available for hits export")

        self.available_hits_fields = available_fields

        # Create request
        request_id = self.create_logs_request('hits', available_fields)

        # Wait for processing
        log_request = self.wait_for_request_processing(request_id)

        # Download data
        df = self.download_data(request_id, log_request['parts'], 'hits')

        # Create table
        self.create_hits_table(available_fields)

        # Upload data
        self.upload_hits_to_clickhouse(df, available_fields)

        logger.info("✓ Hits export completed successfully!\n")

    def export_visits(self):
        """Export visits data"""
        logger.info("\n" + "="*60)
        logger.info("EXPORTING VISITS DATA")
        logger.info("="*60)

        # Validate fields
        available_fields, unavailable_fields = self.validate_fields('visits', self.VISITS_FIELDS)

        if unavailable_fields:
            logger.warning(f"⚠ Some fields are not available and will be skipped:")
            for field in unavailable_fields:
                logger.warning(f"  - {field}")
            logger.info(f"Continuing with {len(available_fields)} available fields")

        if not available_fields:
            raise YandexMetricaAPIError("No fields available for visits export")

        self.available_visits_fields = available_fields

        # Create request
        request_id = self.create_logs_request('visits', available_fields)

        # Wait for processing
        log_request = self.wait_for_request_processing(request_id)

        # Download data
        df = self.download_data(request_id, log_request['parts'], 'visits')

        # Create table
        self.create_visits_table(available_fields)

        # Upload data
        self.upload_visits_to_clickhouse(df, available_fields)

        logger.info("✓ Visits export completed successfully!\n")

    def run(self):
        """Execute the export process"""
        try:
            logger.info("\n" + "="*60)
            logger.info("YANDEX METRICA COMPLETE EXPORT TO CLICKHOUSE")
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
                logger.info(f"  - hits_complete ({len(self.available_hits_fields)} fields)")
            if self.config.get('export_visits', True):
                logger.info(f"  - visits_complete ({len(self.available_visits_fields)} fields)")

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
        description='Complete Yandex Metrica data export to ClickHouse with all available fields',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script exports ALL available fields (~40 for visits, 8 for hits) from Yandex Metrica.
Fields that are not available for your counter are automatically skipped without errors.

Key features:
  - Proper field mapping (fixes clientID=0 issue in old script)
  - Graceful handling of unavailable fields
  - Detailed validation and logging

Tables created:
  - hits_complete (8 fields)
  - visits_complete (~40 fields, depending on availability)

Examples:
  # Using config file
  python export_ym_complete.py --config config.json

  # Using environment variables
  export YM_TOKEN=your_token
  export YM_COUNTER_ID=123456
  export YM_START_DATE=2024-01-01
  export YM_END_DATE=2024-01-31
  export CH_HOST=https://clickhouse.example.com:8443
  export CH_USER=user
  export CH_PASS=password
  export CH_DATABASE=analytics
  python export_ym_complete.py

  # Export only hits or only visits
  export EXPORT_HITS=true
  export EXPORT_VISITS=false
  python export_ym_complete.py --config config.json
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
    exporter = YMCompleteExporter(config)
    success = exporter.run()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
