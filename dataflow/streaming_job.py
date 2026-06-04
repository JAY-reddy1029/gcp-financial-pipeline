#!/usr/bin/env python3
"""
Dataflow Streaming Job
Reads from Pub/Sub, processes stock prices, writes to BigQuery
"""

import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from google.cloud import bigquery

# Configuration
PROJECT_ID = "gcp-financial-pipeline"
TOPIC_ID = "stock-prices-topic"
DATASET_ID = "raw_layer"
TABLE_ID = "stock_prices_raw"

# Set environment variable for BigQuery
os.environ['GOOGLE_CLOUD_PROJECT'] = PROJECT_ID

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParsePubSubMessage(beam.DoFn):
    """Parse JSON message from Pub/Sub"""
    
    def process(self, element):
        try:
            # Decode the Pub/Sub message
            message = json.loads(element.decode('utf-8'))
            
            # Log for debugging
            logger.info(f"Received: {message['symbol']} @ ₹{message['price']}")
            
            # Yield the parsed message
            yield message
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            # Skip malformed messages
        except Exception as e:
            logger.error(f"Error processing message: {e}")


class FormatForBigQuery(beam.DoFn):
    """Format message for BigQuery table"""
    
    def process(self, element):
        try:
            # Create BigQuery row
            row = {
                'timestamp': element.get('timestamp'),
                'symbol': element.get('symbol'),
                'price': float(element.get('price', 0)),
                'volume': int(element.get('volume', 0)),
                'exchange': element.get('exchange'),
                'data_source': element.get('data_source'),
            }
            
            logger.info(f"Writing to BigQuery: {row['symbol']} @ ₹{row['price']}")
            yield row
            
        except Exception as e:
            logger.error(f"Failed to format message: {e}")


def run():
    """Main pipeline function"""
    
    # Pipeline options - Use DirectRunner for local testing
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'
    options.view_as(StandardOptions).streaming = True
    
    # Create BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'price', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'exchange', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'data_source', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }
    
    # Create pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(
                topic=f'projects/{PROJECT_ID}/topics/{TOPIC_ID}'
            )
            | 'ParseMessage' >> beam.ParDo(ParsePubSubMessage())
            | 'FormatForBQ' >> beam.ParDo(FormatForBigQuery())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                schema=table_schema,
                project=PROJECT_ID,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()