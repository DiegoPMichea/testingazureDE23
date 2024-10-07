from azure.storage.queue import QueueClient
import pyodbc
import json
from datetime import datetime
import time
from azure.core.exceptions import ResourceExistsError
import os

# Azure Storage Queue connection string
azure_storage_account_name = os.environ.get('azure_storage_account_name')
azure_storage_account_key = os.environ.get('azure_storage_account_key')
azure_storage_connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_storage_account_name};AccountKey={azure_storage_account_key};EndpointSuffix=core.windows.net"
azure_storage_queue_name = os.environ.get('azure_storage_queue_name')
sql_server = os.environ.get('sql_server')
sql_database = os.environ.get('sql_database')
sql_user = os.environ.get('sql_user')
sql_password = os.environ.get('sql_password')

# Create a QueueClient
queue_client = QueueClient.from_connection_string(azure_storage_connection_string, azure_storage_queue_name)

# SQL Connection details
conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                      'Server=tcp:{sql_server},1433;'
                      'Database={sql_database};'
                      'Uid={sql_user};'
                      'Pwd={sql_password};'  # Replace with the actual password
                      'Encrypt=yes;'
                      'TrustServerCertificate=no;'
                      'Connection Timeout=30;')
cursor = conn.cursor()

def process_message(message):
    # Deserialize the message
    data = json.loads(message.content)
    
    # Convert timestamp to a datetime object
    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
    
    query = "INSERT INTO SensorData (sensor_id, vehicle_count, average_speed, timestamp) VALUES (?, ?, ?, ?)"
    cursor.execute(query, data['sensor_id'], data['vehicle_count'], data['average_speed'], timestamp)
    conn.commit()
    queue_client.delete_message(message)  # Remove the message after processing

def get_queue_message_count():
    try:
        properties = queue_client.get_queue_properties()
        return properties.approximate_message_count
    except ResourceExistsError:
        print("Queue does not exist.")
        return 0

if __name__ == "__main__":
    print(f"Connected to Storage account: {azure_storage_account_name}")
    print(f"Using queue: {azure_storage_queue_name}")
    
    messages_processed = 0
    empty_queue_count = 0
    max_empty_attempts = 3  # Number of empty queue checks before exiting

    while empty_queue_count < max_empty_attempts:
        message_count = get_queue_message_count()
        print(f"Approximate message count in queue: {message_count}")
        
        messages = queue_client.receive_messages(messages_per_page=32, visibility_timeout=30)
        messages_list = list(messages)
        
        if not messages_list:
            empty_queue_count += 1
            print(f"No messages received. Attempt {empty_queue_count}/{max_empty_attempts}")
            print(f"Queue client details: {queue_client.url}")
            time.sleep(5)  # Wait for 5 seconds before checking again
            continue
        
        print(f"Received {len(messages_list)} messages")
        
        empty_queue_count = 0  # Reset the counter if we found messages
        for message in messages_list:
            try:
                process_message(message)
                messages_processed += 1
                print(f"Processed message: {message.id}")
            except Exception as e:
                print(f"Error processing message {message.id}: {e}")
    
    # Close the database connection
    conn.close()
    print(f"No more messages in the queue. Total messages processed: {messages_processed}")
    print("Script execution completed.")
