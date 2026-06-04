#!/usr/bin/env python3
"""
Stock Price Publisher Script
Simulates a stock price API and publishes to Google Cloud Pub/Sub
"""

import json
import time
import random
from datetime import datetime
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "gcp-financial-pipeline"
TOPIC_ID = "stock-prices-topic"

# Stock symbols and price ranges
STOCKS = {
    "RELIANCE": {"base": 2450, "range": 50},
    "INFY": {"base": 1850, "range": 30},
    "TCS": {"base": 3500, "range": 60},
    "WIPRO": {"base": 420, "range": 20},
    "HDFC": {"base": 2650, "range": 40},
}

def publish_stock_price(publisher, topic_path, symbol, price, volume):
    """Publish a single stock price to Pub/Sub"""
    
    # Create message
    message_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "symbol": symbol,
        "price": round(price, 2),
        "volume": volume,
        "exchange": "NSE",
        "data_source": "market-api-simulator"
    }
    
    # Convert to JSON bytes
    message_json = json.dumps(message_data)
    message_bytes = message_json.encode('utf-8')
    
    # Publish
    publish_future = publisher.publish(topic_path, message_bytes)
    message_id = publish_future.result()
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Published {symbol}: ₹{price} (Volume: {volume}) | Message ID: {message_id}")


def main():
    """Main function to continuously publish stock prices"""
    
    # Create publisher client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    print(f"Starting Stock Price Publisher...")
    print(f"Publishing to topic: {topic_path}")
    print(f"Stocks: {', '.join(STOCKS.keys())}")
    print(f"Publishing every 5 seconds...")
    print("-" * 80)
    
    try:
        message_count = 0
        while True:
            # Pick random stock
            symbol = random.choice(list(STOCKS.keys()))
            stock_config = STOCKS[symbol]
            
            # Generate realistic price (random walk)
            price = stock_config["base"] + random.uniform(-stock_config["range"], stock_config["range"])
            volume = random.randint(50000, 500000)
            
            # Publish
            publish_stock_price(publisher, topic_path, symbol, price, volume)
            message_count += 1
            
            # Wait 5 seconds before next message
            time.sleep(5)
            
    except KeyboardInterrupt:
        print(f"\n\nStopped. Total messages published: {message_count}")
        print("Publisher shutting down...")


if __name__ == "__main__":
    main()