import json
import random
from datetime import datetime, timezone
import time
from azure.storage.queue import QueueClient
import logging
import os

def generate_data():
    current_time = datetime.now(timezone.utc)
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    data = {
        "timestamp": formatted_time,
        "sensor_id": random.randint(1, 100),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(0, 120), 1)
    }
    return json.dumps(data)

def send_to_queue(queue_client, data):
    try:
        message = queue_client.send_message(data)
        logging.info(f"Message sent successfully. Message ID: {message.id}")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")
        raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Azure Storage account details
    azure_storage_account_name = os.environ.get('azure_storage_account_name')
    azure_storage_account_key = os.environ.get('azure_storage_account_key')
    azure_storage_queue_name = os.environ.get('azure_storage_queue_name')
    sql_server = os.environ.get('sql_server')
    sql_database = os.environ.get('sql_database')
    sql_user = os.environ.get('sql_user')
    sql_password = os.environ.get('sql_password')

    # Create the connection string
    azure_storage_connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_storage_account_name};AccountKey={azure_storage_account_key};EndpointSuffix=core.windows.net"

    # Create a QueueClient
    queue_client = QueueClient.from_connection_string(azure_storage_connection_string, azure_storage_queue_name)

    try:
        logging.info(f"Connected to Storage account: {azure_storage_account_name}")
        logging.info(f"Using existing queue: {azure_storage_queue_name}")

        while True:
            data = generate_data()
            send_to_queue(queue_client, data)
            logging.info(f"Sent message: {data}")
            time.sleep(1)  # Wait for 1 second before generating next data point
    except KeyboardInterrupt:
        logging.info("\nScript terminated by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")