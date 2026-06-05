#!/usr/bin/env python3
"""
Batch Dataflow Job
Reads CSV from GCS, processes stock prices, writes to BigQuery
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
    """Parse CSV line into dictionary"""
    
    def process(self, element):
        try:
            if element.startswith('timestamp'):  # Skip header
                return
            
            # Split CSV line
            parts = element.split(',')
            
            if len(parts) < 6:
                logger.warning(f"Skipping invalid line: {element}")
                return
            
            # Create row
            row = {
                'timestamp': parts[0],
                'symbol': parts[1],
                'price': float(parts[2]),
                'volume': int(parts[3]),
                'exchange': parts[4],
                'data_source': parts[5].strip(),
                'processed_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            logger.info(f"Parsed: {row['symbol']} @ ₹{row['price']} | {row['timestamp']}")
            yield row
            
        except Exception as e:
            logger.error(f"Error parsing line '{element}': {e}")


class ValidateData(beam.DoFn):
    """Validate data quality"""
    
    def process(self, element):
        try:
            # Validation checks
            assert element['symbol'] in ['RELIANCE', 'INFY', 'TCS', 'WIPRO', 'HDFC'], f"Unknown symbol: {element['symbol']}"
            assert element['price'] > 0, f"Invalid price: {element['price']}"
            assert element['volume'] > 0, f"Invalid volume: {element['volume']}"
            assert element['exchange'] == 'NSE', f"Invalid exchange: {element['exchange']}"
            
            logger.info(f"✓ Validated: {element['symbol']} @ ₹{element['price']}")
            yield element
            
        except AssertionError as e:
            logger.warning(f"Validation failed: {e}")
        except Exception as e:
            logger.error(f"Validation error: {e}")


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
        
        logger.info(f"Batch job completed! Data written to {PROJECT_ID}:{DATASET_ID}.{TABLE_ID}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()