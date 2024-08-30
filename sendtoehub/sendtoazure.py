from azure.eventhub import EventHubProducerClient, EventData
import json

def send_json_to_eventhub(json_file_path, eventhub_connection_str, eventhub_name):
    # Read content from the JSON file
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    # Convert to JSON string
    json_string = json.dumps(json_data)

    # Event Hubs connection information
    connection_str = eventhub_connection_str

    # Create connection client
    producer_client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

    try:
        # Split data into batches to avoid size limit
        for batch_data in split_data(json_string):
            event_data_batch = producer_client.create_batch()
            event_data = EventData(batch_data)
            event_data_batch.add(event_data)
            producer_client.send_batch(event_data_batch)

            print(f'Data has been sent to Event Hubs: {batch_data}')

    finally:
        # Close the connection
        producer_client.close()

def split_data(data, chunk_size=256):
    for i in range(0, len(data), chunk_size):
        yield data[i:i+chunk_size]

# Replace with your actual information
json_file_path = 'C:\\Users\\LE HA\\Downloads\\data_new.json'
eventhub_connection_str = 'Endpoint=sb://is402streamingdata.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=EETo5IGbrx814vLHejnyz/a9TJi2fccQs+AEhFm84+0='
eventhub_name = 'weather-input'

# Call the function to send JSON file to Event Hub
send_json_to_eventhub(json_file_path, eventhub_connection_str, eventhub_name)
 