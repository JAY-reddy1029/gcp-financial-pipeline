#!/usr/bin/env python3
"""
Analytics Dataflow Job
Reads cleaned data from processed_layer, aggregates daily summaries, writes to analytics_layer
Production-grade with retry logic and comprehensive error handling
"""
import logging
import os
import time
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery

# Configuration
PROJECT_ID = "gcp-financial-pipeline"
SOURCE_DATASET = "processed_layer"
SOURCE_TABLE = "stock_prices_cleaned"
TARGET_DATASET = "analytics_layer"
TARGET_TABLE = "daily_price_summary"
GCS_TEMP = "gs://financial-pipeline-staging-gcp-financial-pipeline/temp"

# Set environment variable for BigQuery
os.environ['GOOGLE_CLOUD_PROJECT'] = PROJECT_ID

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DailyPriceSummary(beam.DoFn):
    """Create daily price summary from cleaned data with validation"""
    
    def process(self, element):
        try:
            # Extract date from timestamp (YYYY-MM-DD)
            timestamp = element['timestamp']
            
            # Handle both string and datetime objects
            if isinstance(timestamp, str):
                date = timestamp.split(' ')[0]  # Get YYYY-MM-DD from string
            else:
                # datetime object - convert to string
                date = timestamp.strftime('%Y-%m-%d')
            
            # Validate required fields
            if not all(key in element for key in ['symbol', 'price', 'volume']):
                logger.warning(f"Missing required fields in element: {element}")
                return
            
            symbol = element['symbol']
            price = float(element['price'])
            volume = int(element['volume'])
            
            # Validate data
            if price <= 0:
                logger.warning(f"Invalid price for {symbol}: {price}")
                return
            
            if volume < 0:
                logger.warning(f"Invalid volume for {symbol}: {volume}")
                return
            
            # Create key: (date, symbol) for grouping
            key = f"{date}#{symbol}"
            
            # Yield (key, data) for grouping
            yield (key, {
                'date': date,
                'symbol': symbol,
                'price': price,
                'volume': volume
            })
            
        except ValueError as ve:
            logger.error(f"Data conversion error in element {element}: {ve}")
        except Exception as e:
            logger.error(f"Error processing element {element}: {e}")


class AggregateDaily(beam.DoFn):
    """Aggregate daily prices for each stock with validation"""
    
    def process(self, element):
        try:
            key, values = element
            date, symbol = key.split('#')
            
            prices = []
            volumes = []
            
            for val in values:
                if isinstance(val, dict) and 'price' in val and 'volume' in val:
                    prices.append(val['price'])
                    volumes.append(val['volume'])
                else:
                    logger.warning(f"Invalid value structure: {val}")
            
            if not prices:
                logger.warning(f"No valid prices for {symbol} on {date}")
                return
            
            # Calculate summary metrics
            open_price = prices[0]  # First price of the day
            close_price = prices[-1]  # Last price of the day
            high_price = max(prices)
            low_price = min(prices)
            total_volume = sum(volumes)
            
            # Validate aggregated data
            if high_price < low_price:
                logger.warning(f"Invalid high/low prices for {symbol} on {date}")
                return
            
            row = {
                'date': date,
                'symbol': symbol,
                'open_price': open_price,
                'high_price': high_price,
                'low_price': low_price,
                'close_price': close_price,
                'total_volume': total_volume
            }
            
            logger.info(f"✓ Daily Summary - {symbol} ({date}): Open ₹{open_price}, High ₹{high_price}, Low ₹{low_price}, Close ₹{close_price}, Volume {total_volume}")
            yield row
            
        except ValueError as ve:
            logger.error(f"Data conversion error in aggregation: {ve}")
        except Exception as e:
            logger.error(f"Error aggregating daily data: {e}")


def run():
    """Main pipeline function with retry logic"""
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"[Attempt {retry_count + 1}/{max_retries}] Starting analytics pipeline...")
            
            # Pipeline options
            options = PipelineOptions()
            options.view_as(StandardOptions).runner = 'DirectRunner'
            
            # BigQuery table schema
            table_schema = {
                'fields': [
                    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
                    {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'open_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                    {'name': 'high_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                    {'name': 'low_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                    {'name': 'close_price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                    {'name': 'total_volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                ]
            }
            
            # Create pipeline
            with beam.Pipeline(options=options) as pipeline:
                (
                    pipeline
                    | 'ReadFromBigQuery' >> ReadFromBigQuery(
                        table=f'{PROJECT_ID}:{SOURCE_DATASET}.{SOURCE_TABLE}',
                        project=PROJECT_ID,
                        gcs_location=GCS_TEMP
                    )
                    | 'CreateDailySummary' >> beam.ParDo(DailyPriceSummary())
                    | 'GroupByDateSymbol' >> beam.GroupByKey()
                    | 'AggregateDaily' >> beam.ParDo(AggregateDaily())
                    | 'WriteToBigQuery' >> WriteToBigQuery(
                        table=f'{PROJECT_ID}:{TARGET_DATASET}.{TARGET_TABLE}',
                        schema=table_schema,
                        project=PROJECT_ID,
                        custom_gcs_temp_location=GCS_TEMP,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    )
                )
            
            logger.info(f"✓ Analytics job completed successfully! Data written to {PROJECT_ID}:{TARGET_DATASET}.{TARGET_TABLE}")
            return True
        
        except Exception as e:
            retry_count += 1
            logger.error(f"[Attempt {retry_count}/{max_retries}] Analytics job failed: {str(e)}")
            
            if retry_count < max_retries:
                wait_time = 5 * retry_count  # Exponential backoff: 5s, 10s, 15s
                logger.info(f"Retrying analytics job in {wait_time} seconds (attempt {retry_count + 1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                logger.error(f"✗ Analytics job failed after {max_retries} attempts. Aborting.")
                raise
    
    return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()