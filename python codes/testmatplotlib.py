import pyodbc
import matplotlib.pyplot as plt
import os
import pandas as pd

# SQL Connection details
sql_server = os.environ.get('sql_server')
sql_database = os.environ.get('sql_database')
sql_user = os.environ.get('sql_user')
sql_password = os.environ.get('sql_password')

connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_server};Database={sql_database};UID={sql_user};PWD={sql_password}"
# Create a connection to the SQL database
conn = pyodbc.connect(connection_string)

def fetch_data():
    query = "SELECT sensor_id, vehicle_count, average_speed, timestamp FROM SensorData"
    return pd.read_sql(query, conn)

def plot_data(data):
    plt.figure(figsize=(12, 6))  # Increased figure size for better visibility
    
    # Calculate total vehicle count by timestamp
    total_vehicle_count = data.groupby('timestamp')['vehicle_count'].sum().reset_index()
    
    # Line chart for total vehicle count over time
    plt.plot(total_vehicle_count['timestamp'], total_vehicle_count['vehicle_count'], marker='o', color='green', label='Total Vehicle Count')
    
    plt.title('Vehicle Count (Over Time)')
    plt.xlabel('Timestamp')
    plt.ylabel('Total Vehicle Count')
    plt.xticks(rotation=45)
    plt.legend()
    
    plt.tight_layout()  # Adjust layout
    plt.show()

if __name__ == "__main__":
    data = fetch_data()
    plot_data(data)
    conn.close()
