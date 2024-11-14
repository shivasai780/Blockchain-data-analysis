import json
import base64
import pymysql
import boto3

# RDS Database Configuration
rds_host = "database-1.cf0sg44ga981.ap-south-1.rds.amazonaws.com"  # Hostname for the RDS MySQL instance
username = "Shivaadmin"  # Database username
password = "Shiva780"    # Database password
database_name = "blkchaindb"  # Target database name

# Function to initialize and return an RDS database connection
def get_db_connection():
    return pymysql.connect(
        host=rds_host,
        user=username,
        password=password,
        db=database_name,
        cursorclass=pymysql.cursors.DictCursor  # Returns query results as dictionaries
    )

# Lambda entry point
def lambda_handler(event, context):
    print("event collected is {}".format(event))  # Log the incoming event for debugging
    
    # Get a connection to the RDS database
    connection = get_db_connection()
    try:
        # Iterate over each record in the event batch
        for record in event['Records']:
            # Decode the base64-encoded data from Kinesis
            sample_string_bytes = base64.b64decode(record['kinesis']['data'])
            sample_string = sample_string_bytes.decode("ascii")  # Convert bytes to string
            event_data = json.loads(sample_string)  # Parse the string as JSON data
            
            # Determine the event type and route to appropriate handler
            event_type = event_data.get('eventType')
            
            if event_type == 'nftCreation':
                insert_nft_creation(event_data, connection)  # Process NFT creation events
            elif event_type == 'nftTransaction':
                insert_nft_transaction(event_data, connection)  # Process NFT transaction events
                
    finally:
        # Ensure the database connection is closed after processing
        connection.close()

# Function to insert NFT creation events into the database
def insert_nft_creation(event_data, connection):
    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO nft_metadata (nft_id, name, attributes)
                VALUES (%s, %s, %s)
            """
            # Execute the SQL insert command with parameters from event data
            cursor.execute(sql, (event_data['nft_id'], event_data['name'], json.dumps(event_data['attributes'])))
        connection.commit()  # Commit the transaction
        print("NFT creation event inserted successfully.")
    except Exception as e:
        print(f"Error inserting NFT creation event: {e}")  # Log any errors

# Function to insert NFT transaction events into the database
def insert_nft_transaction(event_data, connection):
    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO nft_transactions (transaction_id, nft_id, seller, buyer, price, transaction_type, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Execute the SQL insert command with parameters from event data
            cursor.execute(sql, (
                event_data['transaction_id'],
                event_data['nft_id'],
                event_data['seller'],
                event_data['buyer'],
                event_data['price'],
                event_data['transaction_type'],
                event_data['timestamp']
            ))
        connection.commit()  # Commit the transaction
        print("NFT transaction event inserted successfully.")
    except Exception as e:
        print(f"Error inserting NFT transaction event: {e}")  # Log any errors
