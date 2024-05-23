import boto3
import csv
import re
import logging
from io import StringIO
import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print('Test Test Print')
    logger.error('Test Test')
    
    # Get the bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Extract deckunit number from the object key
    deckunit_number = object_key.split('/')[0]
    
    logger.error(f'DU Number: {deckunit_number}')
    
    # Check if the object is a CSV file
    if object_key.lower().endswith('.csv'):
        logger.error(f'File object key is: {object_key}')
        try:
            # Read the CSV file from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_content = response['Body'].read().decode('utf-8').splitlines()
        
            # Initialize variables
            battery_percent = 0
            last_comm = None
            
            # Search for the row containing the battery percentage
            for row in csv_content:
                columns = row.split(',')
                if columns[0].strip() == 'Deck unit battery percent':
                    battery_percent = float(columns[1])
                elif columns[0].strip().strip() == 'Upload time':
                    try:
                        last_comm = datetime.datetime.strptime(columns[1].strip(), '%Y%m%dT%H%M%S').strftime('%d %b %Y')
                    except ValueError as e:
                        logger.error(f"Error parsing date: {e}")
                
            if battery_percent is not None and last_comm is not None:
                print(f'Battery percentage: {battery_percent:.1f}%')
                print(f'Date of Last comm: {last_comm}')
                update_html_table(deckunit_number, battery_percent, last_comm)
            else:
                logger.error("Battery percentage or last communication date not found in the CSV.")
        
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")

    else:
        logger.error(f"Object {object_key} is not a CSV file.")

def update_html_table(deckunit_number, battery_percent, last_comm):
    try:
        # Read the existing HTML file from S3
        html_bucket = 'fishsoop-webstats'
        html_object_key = 'fishsoop-web-stats.html'
        response = s3.get_object(Bucket=html_bucket, Key=html_object_key)
        existing_html = response['Body'].read().decode('utf-8')
        
        # Extract boat name and other relevant details
        boat_name = extract_boat_name(deckunit_number)
        logger.error(f'Vessel Name: {boat_name}')
        if not boat_name:
            logger.error(f"Boat name not found for DU number: {deckunit_number}")
            return
        
        # Update the HTML table
        updated_html = update_html(existing_html, deckunit_number, boat_name, battery_percent, last_comm)
        
        # Upload the updated HTML file back to S3
        s3.put_object(Bucket=html_bucket, Key=html_object_key, Body=updated_html.encode('utf-8'))
        
        logger.error("HTML file updated successfully.")
    except Exception as e:
        logger.error(f"Error updating HTML file: {e}")

def update_html(existing_html, du_number, boat_name, battery_percent, last_comm):
    # Find the row with the specified boat name
    existing_row_match = re.search(
        rf'<tr>\s*<td><span class="(?:k1|o1|r1)">{re.escape(boat_name)}</span></td>.*?</tr>',
        existing_html,
        flags=re.DOTALL
    )

    if not existing_row_match:
        logger.error(f'Boat {boat_name} not in existing boat list')
        return existing_html  # Boat name not found, no update needed

    existing_row = existing_row_match.group(0)
    logger.error(f'Existing row is: {existing_row}')
    
    # Extract the values and classes of the columns using regex
    column_matches = re.findall(r'<td><span class="([^"]*)">([^<]*)</span></td>', existing_row)
    columns = [(match[0], match[1]) for match in column_matches]
    
    if len(columns) >= 6:
        boat_name_class, boat_name_col = columns[0]
        sensor_class, sensor_nu = columns[1]  # 2nd column
        date_class, latest_data = columns[2]  # 3rd column

        # Determine the class for the battery percentage based on its value
        if float(battery_percent) < 60:
            battery_class = 'r1'
        elif float(battery_percent) < 80:
            battery_class = 'o1'
        else:
            battery_class = 'k1'

        # Determine the class for the last communication date based on the time difference
        last_comm_date = datetime.datetime.strptime(last_comm, '%d %b %Y')
        now = datetime.datetime.now()
        time_diff = now - last_comm_date
        
        if time_diff.days > 14:
            last_comm_class = 'r1'
        elif time_diff.days > 2:
            last_comm_class = 'o1'
        else:
            last_comm_class = 'k1'

        # Create the updated row with the original classes for the first three columns and updated classes for the last two columns
        updated_row = (
            f'<tr>'
            f'<td><span class="{boat_name_class}">{boat_name_col}</span></td>'
            f'<td><span class="{sensor_class}">{sensor_nu}</span></td>'
            f'<td><span class="{date_class}">{latest_data}</span></td>'
            f'<td><span class="k1">{du_number}</span></td>'
            f'<td><span class="{battery_class}">{battery_percent}</span></td>'
            f'<td><span class="{last_comm_class}">{last_comm}</span></td>'
            f'</tr>'
        )
        
        logger.error(f'Updated row is: {updated_row}')
        
        # Replace the old row with the updated row
        updated_html = existing_html.replace(existing_row, updated_row)
    else:
        logger.error('Unexpected row format or missing columns')
        updated_html = existing_html
    
    return updated_html

def extract_boat_name(deckunit_number):
    # Read the CSV file containing boat information from S3
    fishers_metafile = 'Trial_fisherman_database_ausTest.csv'
    meta_bucket = 'fishsoop-qc-tools'
    
    try:
        response = s3.get_object(Bucket=meta_bucket, Key=fishers_metafile)
        csv_content = response['Body'].read().decode('utf-8').splitlines()
        
        reader = csv.DictReader(csv_content[1:])  # Skip header
        for row in reader:
            if row['Deck unit serial number'].lstrip('0') == deckunit_number and row['Active/Terminated'] == 'Active':
                logger.error(f'Found deck unit in metadata file')
                
                return row['Vessel name']
        return None
    except Exception as e:
        logger.error(f"Error reading boat info CSV file: {e}")
        return None
