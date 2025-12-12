#!/usr/bin/env python3
"""
Georgia House Flip Pipeline
Scrapes property data from Georgia counties and identifies house flips.
"""
import sys
import os
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

# Optional dependency - graceful fallback
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Fallback: simple passthrough
    def tqdm(iterable, **kwargs):
        return iterable

from src.scraper import GeorgiaPropertyScraper
from src.analyzer import FlipAnalyzer


# Configuration
DEFAULT_OUTPUT_DIR = 'data'
DEFAULT_CONFIG_PATH = 'config/counties.json'
DEFAULT_MAX_WORKERS = 4
REQUIRED_COLUMNS = ['county', 'profit', 'hold_days', 'roi']


class PipelineConfig:
    """Configuration container for pipeline settings"""
    def __init__(
        self,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        config_path: str = DEFAULT_CONFIG_PATH,
        max_workers: int = DEFAULT_MAX_WORKERS,
        counties_filter: Optional[List[str]] = None,
        verbose: bool = False
    ):
        self.output_dir = Path(output_dir)
        self.config_path = Path(config_path)
        self.max_workers = max_workers
        self.counties_filter = counties_filter
        self.verbose = verbose
        
        # Create output directories
        self.raw_dir = self.output_dir / 'raw'
        self.raw_dir.mkdir(parents=True, exist_ok=True)


def setup_logging(verbose: bool = False, log_file: str = 'pipeline.log') -> logging.Logger:
    """
    Configure logging for the pipeline.
    
    Args:
        verbose: If True, set log level to DEBUG
        log_file: Path to log file
        
    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    logger = logging.getLogger('pipeline')
    logger.setLevel(log_level)
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    try:
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except (IOError, PermissionError) as e:
        logger.warning(f"Could not create log file {log_file}: {e}")
    
    return logger


def load_county_configs(config_path: Path, logger: logging.Logger) -> Dict:
    """
    Load and validate county configuration file.
    
    Args:
        config_path: Path to counties.json
        logger: Logger instance
        
    Returns:
        Dictionary of county configurations
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    logger.info(f"Loading county configurations from {config_path}")
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    if 'counties' not in config_data:
        raise ValueError("Configuration file must contain 'counties' key")
    
    counties = config_data['counties']
    
    # Validate each county config
    for county_id, config in counties.items():
        if 'name' not in config:
            raise ValueError(f"County {county_id} missing 'name' field")
    
    logger.info(f"Loaded {len(counties)} county configurations")
    return counties


def scrape_single_county(
    county_id: str,
    config: Dict,
    logger: logging.Logger
) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    """
    Scrape data from a single county.
    
    Args:
        county_id: County identifier
        config: County configuration dictionary
        logger: Logger instance
        
    Returns:
        Tuple of (county_id, dataframe, error_message)
    """
    county_name = config.get('name', county_id)
    
    try:
        logger.info(f"Scraping {county_name}...")
        scraper = GeorgiaPropertyScraper(config)
        data = scraper.get_recent_sales()
        
        if data is None or data.empty:
            logger.warning(f"No data returned for {county_name}")
            return county_id, None, None
        
        logger.info(f"Found {len(data)} records for {county_name}")
        return county_id, data, None
        
    except Exception as e:
        error_msg = f"Error scraping {county_name}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return county_id, None, error_msg


def scrape_counties(
    counties: Dict,
    config: PipelineConfig,
    logger: logging.Logger
) -> Tuple[List[pd.DataFrame], List[str]]:
    """
    Scrape all enabled counties, optionally in parallel.
    
    Args:
        counties: Dictionary of county configurations
        config: Pipeline configuration
        logger: Logger instance
        
    Returns:
        Tuple of (list of dataframes, list of error messages)
    """
    all_data = []
    errors = []
    
    # Filter counties if specified
    counties_to_scrape = {
        k: v for k, v in counties.items()
        if v.get('enabled', True) and (
            config.counties_filter is None or 
            k in config.counties_filter or 
            v.get('name') in config.counties_filter
        )
    }
    
    logger.info(f"Scraping {len(counties_to_scrape)} counties...")
    
    if config.max_workers == 1:
        # Sequential processing
        for county_id, county_config in tqdm(
            counties_to_scrape.items(),
            desc="Scraping counties",
            disable=not sys.stdout.isatty()
        ):
            _, data, error = scrape_single_county(county_id, county_config, logger)
            if data is not None:
                all_data.append(data)
            if error:
                errors.append(error)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {
                executor.submit(scrape_single_county, cid, cfg, logger): cid
                for cid, cfg in counties_to_scrape.items()
            }
            
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Scraping counties",
                disable=not sys.stdout.isatty()
            ):
                _, data, error = future.result()
                if data is not None:
                    all_data.append(data)
                if error:
                    errors.append(error)
    
    return all_data, errors


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> None:
    """
    Validate that dataframe has required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Raises:
        ValueError: If required columns are missing
    """
    if df.empty:
        return
    
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def calculate_statistics(
    flips: pd.DataFrame,
    investors: pd.DataFrame,
    logger: logging.Logger
) -> Dict:
    """
    Calculate summary statistics from flip data.
    
    Args:
        flips: DataFrame of identified flips
        investors: DataFrame of investor analysis
        logger: Logger instance
        
    Returns:
        Dictionary of statistics
    """
    logger.info("Calculating statistics...")
    
    stats = {
        'total_flips_identified': len(flips),
        'total_investors': len(investors),
        'total_profit': 0,
        'avg_hold_days': 0,
        'avg_roi': 0,
        'by_county': {},
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if len(flips) > 0:
        try:
            stats['total_profit'] = float(flips['profit'].sum())
            stats['avg_hold_days'] = float(flips['hold_days'].mean())
            stats['avg_roi'] = float(flips['roi'].mean())
            stats['by_county'] = flips['county'].value_counts().to_dict()
        except KeyError as e:
            logger.error(f"Missing column in flips data: {e}")
        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
    
    return stats


def save_results(
    combined_data: pd.DataFrame,
    flips: pd.DataFrame,
    investors: pd.DataFrame,
    stats: Dict,
    config: PipelineConfig,
    logger: logging.Logger
) -> None:
    """
    Save all results to disk.
    
    Args:
        combined_data: Combined raw scraped data
        flips: Identified flips
        investors: Investor analysis
        stats: Summary statistics
        config: Pipeline configuration
        logger: Logger instance
    """
    logger.info("Saving results...")
    
    try:
        # Save dashboard data
        dashboard_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_flips': len(flips),
                'total_investors': len(investors),
                'data_source': 'Georgia County Public Records'
            },
            'stats': stats,
            'recent_flips': flips.head(100).to_dict('records') if len(flips) > 0 else [],
            'top_investors': investors.head(50).to_dict('records') if len(investors) > 0 else []
        }
        
        dashboard_path = config.output_dir / 'dashboard_data.json'
        with open(dashboard_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        logger.info(f"Saved dashboard data to {dashboard_path}")
        
        # Save CSV files
        if len(flips) > 0:
            flips_path = config.output_dir / 'flips.csv'
            flips.to_csv(flips_path, index=False)
            logger.info(f"Saved {len(flips)} flips to {flips_path}")
        
        if len(investors) > 0:
            investors_path = config.output_dir / 'investors.csv'
            investors.to_csv(investors_path, index=False)
            logger.info(f"Saved {len(investors)} investors to {investors_path}")
        
        # Save raw data
        raw_path = config.raw_dir / f'scrape_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        combined_data.to_csv(raw_path, index=False)
        logger.info(f"Saved raw data to {raw_path}")
        
        # Create README
        readme_content = f"""# Georgia House Flip Data

## Latest Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Summary
- Total Flips Identified: {len(flips):,}
- Total Investors: {len(investors):,}
- Total Profit: ${stats['total_profit']:,.2f}
- Average ROI: {stats['avg_roi']:.1f}%
- Average Hold Days: {stats['avg_hold_days']:.0f}

### Files
- `dashboard_data.json` - Complete data for dashboard
- `flips.csv` - All detected flips
- `investors.csv` - All identified investors
- `raw/` - Raw scraped data with timestamps

### Flip Detection Criteria
- Hold Period: 30-365 days
- Profit Range: $70,000-$150,000

### Data Sources
{', '.join(sorted(stats.get('by_county', {}).keys()))}

---
Generated by Georgia House Flip Pipeline
"""
        
        readme_path = config.output_dir / 'README.md'
        with open(readme_path, 'w') as f:
            f.write(readme_content)
        logger.info(f"Saved README to {readme_path}")
        
    except (IOError, PermissionError) as e:
        logger.error(f"Error saving results: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error saving results: {e}", exc_info=True)
        raise


def run_pipeline(config: PipelineConfig) -> int:
    """
    Main pipeline execution function.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger = setup_logging(verbose=config.verbose)
    
    logger.info("=" * 60)
    logger.info("Georgia House Flip Pipeline")
    logger.info(f"Started: {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        # Load county configurations
        counties = load_county_configs(config.config_path, logger)
        
        # Scrape counties
        all_data, errors = scrape_counties(counties, config, logger)
        
        if errors:
            logger.warning(f"Encountered {len(errors)} errors during scraping")
            for error in errors[:5]:  # Show first 5 errors
                logger.warning(f"  {error}")
            if len(errors) > 5:
                logger.warning(f"  ... and {len(errors) - 5} more errors")
        
        if not all_data:
            logger.error("No data scraped from any county")
            return 1
        
        # Combine all data
        logger.info(f"Combining data from {len(all_data)} counties...")
        combined_data = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total records: {len(combined_data):,}")
        
        # Analyze for flips
        logger.info("Analyzing for house flips...")
        analyzer = FlipAnalyzer()
        flips = analyzer.identify_flips(combined_data)
        logger.info(f"Identified {len(flips):,} potential flips")
        
        # Validate flip data
        if len(flips) > 0:
            validate_dataframe(flips, REQUIRED_COLUMNS)
        
        # Analyze investors
        logger.info("Analyzing investors...")
        investors = analyzer.analyze_investors(flips)
        logger.info(f"Identified {len(investors):,} investors")
        
        # Calculate statistics
        stats = calculate_statistics(flips, investors, logger)
        
        # Save results
        save_results(combined_data, flips, investors, stats, config, logger)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Flips Found: {len(flips):,}")
        logger.info(f"Investors: {len(investors):,}")
        logger.info(f"Total Profit: ${stats['total_profit']:,.2f}")
        logger.info(f"Average ROI: {stats['avg_roi']:.1f}%")
        logger.info(f"Files saved to {config.output_dir}/ directory")
        logger.info("=" * 60)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}", exc_info=True)
        return 1


def main():
    """Parse arguments and run pipeline."""
    parser = argparse.ArgumentParser(
        description='Georgia House Flip Detection Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python run_pipeline.py
  
  # Scrape specific counties only
  python run_pipeline.py --counties Fulton DeKalb Cobb
  
  # Use custom output directory
  python run_pipeline.py --output-dir /path/to/output
  
  # Enable verbose logging
  python run_pipeline.py --verbose
  
  # Use parallel processing with 8 workers
  python run_pipeline.py --workers 8
        """
    )
    
    parser.add_argument(
        '--counties',
        nargs='+',
        metavar='COUNTY',
        help='Specific counties to scrape (by ID or name)'
    )
    
    parser.add_argument(
        '--output-dir',
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '--config',
        default=DEFAULT_CONFIG_PATH,
        help=f'Path to counties config file (default: {DEFAULT_CONFIG_PATH})'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f'Number of parallel workers (default: {DEFAULT_MAX_WORKERS}, use 1 for sequential)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose debug logging'
    )
    
    parser.add_argument(
        '--log-file',
        default='pipeline.log',
        help='Path to log file (default: pipeline.log)'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config = PipelineConfig(
        output_dir=args.output_dir,
        config_path=args.config,
        max_workers=args.workers,
        counties_filter=args.counties,
        verbose=args.verbose
    )
    
    # Run pipeline
    exit_code = run_pipeline(config)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
