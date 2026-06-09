#!/usr/bin/env python3
"""
Batch Dataflow Job
Reads CSV from GCS, processes stock prices, writes to BigQuery
Production-grade with enhanced data validation and error handling
"""
import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
from datetime import datetime

# Configuration
PROJECT_ID = "gcp-financial-pipeline"
INPUT_BUCKET = "gs://financial-pipeline-staging-gcp-financial-pipeline/input/historical_prices.csv"
DATASET_ID = "processed_layer"
TABLE_ID = "stock_prices_cleaned"

# Set environment variable for BigQuery
os.environ['GOOGLE_CLOUD_PROJECT'] = PROJECT_ID

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseCSVLine(beam.DoFn):
    """Parse CSV line into dictionary with error handling"""
    
    def process(self, element):
        try:
            if element.startswith('timestamp'):  # Skip header
                return
            
            # Split CSV line
            parts = element.split(',')
            
            if len(parts) < 6:
                logger.warning(f"Skipping invalid line (insufficient fields): {element}")
                return
            
            # Create row
            row = {
                'timestamp': parts[0].strip(),
                'symbol': parts[1].strip(),
                'price': float(parts[2].strip()),
                'volume': int(parts[3].strip()),
                'exchange': parts[4].strip(),
                'data_source': parts[5].strip(),
                'processed_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            logger.info(f"Parsed: {row['symbol']} @ ₹{row['price']} | {row['timestamp']}")
            yield row
            
        except ValueError as ve:
            logger.error(f"Type conversion error in line '{element}': {ve}")
        except Exception as e:
            logger.error(f"Error parsing line '{element}': {e}")


class ValidateData(beam.DoFn):
    """Validate data quality with detailed checks"""
    
    def process(self, element):
        try:
            # Validate required fields exist and are not None
            required_fields = ['timestamp', 'symbol', 'price', 'volume', 'exchange', 'data_source']
            for field in required_fields:
                if field not in element or element[field] is None:
                    logger.warning(f"Missing required field '{field}' in record: {element}")
                    return
            
            # Validate symbol is one of known stocks
            valid_symbols = ['RELIANCE', 'INFY', 'TCS', 'WIPRO', 'HDFC']
            if element['symbol'] not in valid_symbols:
                logger.warning(f"Unknown symbol: {element['symbol']}")
                return
            
            # Validate price is positive
            if not isinstance(element['price'], (int, float)):
                logger.warning(f"Invalid price type: {type(element['price'])}")
                return
            
            if element['price'] <= 0:
                logger.warning(f"Invalid price (must be positive): {element['price']}")
                return
            
            # Validate volume is non-negative
            if not isinstance(element['volume'], int):
                logger.warning(f"Invalid volume type: {type(element['volume'])}")
                return
            
            if element['volume'] < 0:
                logger.warning(f"Invalid volume (must be non-negative): {element['volume']}")
                return
            
            # Validate exchange
            valid_exchanges = ['NSE', 'BSE']
            if element['exchange'] not in valid_exchanges:
                logger.warning(f"Invalid exchange: {element['exchange']}")
                return
            
            # Validate timestamp format
            try:
                datetime.fromisoformat(element['timestamp'].replace('Z', '+00:00'))
            except ValueError:
                logger.warning(f"Invalid timestamp format: {element['timestamp']}")
                return
            
            logger.info(f"✓ Validated: {element['symbol']} @ ₹{element['price']} | Volume: {element['volume']}")
            yield element
            
        except Exception as e:
            logger.error(f"Validation error for record {element}: {e}")


def run():
    """Main pipeline function"""
    
    # Pipeline options - Use DirectRunner for local testing
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'
    
    # Create BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'exchange', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'data_source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ]
    }
    
    try:
        # Create pipeline
        with beam.Pipeline(options=options) as pipeline:
            (
                pipeline
                | 'ReadCSV' >> ReadFromText(INPUT_BUCKET)
                | 'ParseCSV' >> beam.ParDo(ParseCSVLine())
                | 'ValidateData' >> beam.ParDo(ValidateData())
                | 'WriteToBigQuery' >> WriteToBigQuery(
                    table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                    schema=table_schema,
                    project=PROJECT_ID,
                    custom_gcs_temp_location='gs://financial-pipeline-staging-gcp-financial-pipeline/temp',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
            )
            
            logger.info(f"✓ Batch job completed successfully! Data written to {PROJECT_ID}:{DATASET_ID}.{TABLE_ID}")
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()