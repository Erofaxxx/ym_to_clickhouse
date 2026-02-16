#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Program to query ClickHouse and display results in a beautiful format.
Mimics Jupyter notebook output but works in terminal or as standalone script.
"""

import os
import sys
import json
import logging
import argparse
from typing import Optional

import pandas as pd
from tabulate import tabulate
from colorama import init, Fore, Style

from some_funcs import simple_ch_client

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseQueryError(Exception):
    """Custom exception for ClickHouse query errors"""
    pass


class BeautifulClickHouseViewer:
    """Query ClickHouse and display results in a beautiful format"""

    def __init__(self, config):
        """
        Initialize viewer with configuration

        Args:
            config (dict): Configuration dictionary with keys:
                - ch_host: ClickHouse host URL
                - ch_user: ClickHouse user
                - ch_pass: ClickHouse password
                - ch_cacert: Path to CA certificate
        """
        self.config = config
        self.ch_client = None

    def validate_config(self):
        """Validate required configuration parameters"""
        required_keys = ['ch_host', 'ch_user', 'ch_pass', 'ch_cacert']

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
            raise ClickHouseQueryError(f"Failed to connect to ClickHouse: {e}")

    def query(self, sql_query, limit=None):
        """
        Execute SQL query and return DataFrame

        Args:
            sql_query (str): SQL query to execute
            limit (int, optional): Limit number of rows

        Returns:
            pd.DataFrame: Query results
        """
        try:
            # Add LIMIT clause if specified and not already in query
            if limit and 'LIMIT' not in sql_query.upper():
                sql_query = f"{sql_query.rstrip(';')} LIMIT {limit}"

            logger.info(f"Executing query: {sql_query[:100]}...")

            df = self.ch_client.get_clickhouse_df(sql_query)

            logger.info(f"Query returned {len(df)} rows, {len(df.columns)} columns")

            return df

        except Exception as e:
            raise ClickHouseQueryError(f"Query execution failed: {e}")

    def display_dataframe(self, df, table_format='grid', max_col_width=50):
        """
        Display DataFrame in a beautiful format

        Args:
            df (pd.DataFrame): DataFrame to display
            table_format (str): Table format for tabulate
            max_col_width (int): Maximum column width for display
        """
        if df.empty:
            print(f"\n{Fore.YELLOW}No data to display{Style.RESET_ALL}\n")
            return

        # Print header
        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}Query Results{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

        # Print shape info
        print(f"{Fore.YELLOW}Shape: {df.shape[0]} rows × {df.shape[1]} columns{Style.RESET_ALL}\n")

        # Truncate long strings for better display
        df_display = df.copy()
        for col in df_display.select_dtypes(include=['object']):
            df_display[col] = df_display[col].apply(
                lambda x: (str(x)[:max_col_width] + '...') if isinstance(x, str) and len(str(x)) > max_col_width else x
            )

        # Display using tabulate
        table = tabulate(
            df_display,
            headers='keys',
            tablefmt=table_format,
            showindex=True,
            numalign='right',
            stralign='left'
        )

        print(table)

        # Print footer
        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")

    def display_statistics(self, df):
        """
        Display basic statistics about the DataFrame

        Args:
            df (pd.DataFrame): DataFrame to analyze
        """
        if df.empty:
            return

        print(f"{Fore.GREEN}Column Information:{Style.RESET_ALL}\n")

        # Column types and non-null counts
        info_data = []
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            null = df[col].isna().sum()
            null_pct = (null / len(df) * 100) if len(df) > 0 else 0

            info_data.append([
                col,
                str(dtype),
                non_null,
                null,
                f"{null_pct:.1f}%"
            ])

        info_table = tabulate(
            info_data,
            headers=['Column', 'Type', 'Non-Null', 'Null', 'Null %'],
            tablefmt='grid'
        )

        print(info_table)
        print()

        # Numeric column statistics
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print(f"{Fore.GREEN}Numeric Column Statistics:{Style.RESET_ALL}\n")
            print(df[numeric_cols].describe().to_string())
            print()

    def run_interactive(self):
        """Run interactive query mode"""
        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}Beautiful ClickHouse Query Tool - Interactive Mode{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}\n")
        print("Enter SQL queries (type 'exit' or 'quit' to exit, 'help' for help)\n")

        while True:
            try:
                # Read query
                print(f"{Fore.YELLOW}SQL>{Style.RESET_ALL} ", end='')
                query = input().strip()

                if not query:
                    continue

                if query.lower() in ['exit', 'quit']:
                    print(f"\n{Fore.GREEN}Goodbye!{Style.RESET_ALL}\n")
                    break

                if query.lower() == 'help':
                    self.print_help()
                    continue

                # Execute query
                df = self.query(query)

                # Display results
                self.display_dataframe(df)

                # Ask if user wants statistics
                print(f"{Fore.YELLOW}Show statistics? (y/n):{Style.RESET_ALL} ", end='')
                show_stats = input().strip().lower()
                if show_stats == 'y':
                    self.display_statistics(df)

            except KeyboardInterrupt:
                print(f"\n\n{Fore.GREEN}Goodbye!{Style.RESET_ALL}\n")
                break
            except ClickHouseQueryError as e:
                print(f"\n{Fore.RED}Error: {e}{Style.RESET_ALL}\n")
            except Exception as e:
                print(f"\n{Fore.RED}Unexpected error: {e}{Style.RESET_ALL}\n")

    def print_help(self):
        """Print help message"""
        help_text = f"""
{Fore.GREEN}Available Commands:{Style.RESET_ALL}
  - Enter any SQL query to execute it
  - 'exit' or 'quit' - Exit the program
  - 'help' - Show this help message

{Fore.GREEN}Example Queries:{Style.RESET_ALL}
  SELECT * FROM database.table LIMIT 10
  SELECT COUNT(*) FROM database.table
  SELECT date, COUNT(*) as visits FROM database.table GROUP BY date ORDER BY date LIMIT 100

{Fore.GREEN}Tips:{Style.RESET_ALL}
  - Queries are automatically limited if no LIMIT clause is specified in non-interactive mode
  - Long strings are truncated for better display
  - Use statistics view to see column information and numeric summaries
        """
        print(help_text)


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
        'ch_host': os.getenv('CH_HOST'),
        'ch_user': os.getenv('CH_USER'),
        'ch_pass': os.getenv('CH_PASS'),
        'ch_cacert': os.getenv('CH_CACERT', 'YandexInternalRootCA.crt')
    }


def main():
    parser = argparse.ArgumentParser(
        description='Query ClickHouse and display results beautifully',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode with config file
  python query_clickhouse.py --config config.json --interactive

  # Query with environment variables
  export CH_HOST=https://clickhouse.example.com:8443
  export CH_USER=user
  export CH_PASS=password
  python query_clickhouse.py --query "SELECT * FROM database.table" --limit 100

  # Query specific table with statistics
  python query_clickhouse.py --table database.ym_visits --limit 100 --stats
        """
    )

    parser.add_argument(
        '--config',
        help='Path to JSON configuration file',
        default=None
    )

    parser.add_argument(
        '--query',
        help='SQL query to execute',
        default=None
    )

    parser.add_argument(
        '--table',
        help='Table name to query (will execute SELECT * FROM table)',
        default=None
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rows (default: 100)',
        default=100
    )

    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Run in interactive mode'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show statistics about the data'
    )

    parser.add_argument(
        '--format',
        choices=['grid', 'fancy_grid', 'simple', 'plain', 'html', 'latex'],
        default='grid',
        help='Table format (default: grid)'
    )

    args = parser.parse_args()

    # Load configuration
    if args.config:
        config = load_config_from_file(args.config)
    else:
        config = load_config_from_env()

    try:
        # Create viewer
        viewer = BeautifulClickHouseViewer(config)
        viewer.validate_config()
        viewer.init_clickhouse_client()

        if args.interactive:
            # Interactive mode
            viewer.run_interactive()
        else:
            # Single query mode
            if args.query:
                query = args.query
            elif args.table:
                query = f"SELECT * FROM {args.table}"
            else:
                logger.error("Either --query or --table must be specified in non-interactive mode")
                sys.exit(1)

            # Execute query
            df = viewer.query(query, limit=args.limit)

            # Display results
            viewer.display_dataframe(df, table_format=args.format)

            # Show statistics if requested
            if args.stats:
                viewer.display_statistics(df)

    except (ClickHouseQueryError, ValueError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
