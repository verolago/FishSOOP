import boto3
import csv
import datetime
import re
import logging

s3 = boto3.client('s3')

def get_sensor_number_from_event(event):
    if event and 'Records' in event:
        records = event['Records']
        if records and len(records) > 0:
            record = records[0]
            if 's3' in record:
                s3_record = record['s3']
                if 'object' in s3_record:
                    object_key = s3_record['object']['key']
                    return object_key.split('/')[0].lstrip('0')
    return None

def read_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8').splitlines()

def get_boat_info(sensor_number, csv_data):
    reader = csv.DictReader(csv_data[1:])  # Skip header
    for row in reader:
        if row['Mangopare serial number'].lstrip('0') == sensor_number and row['Active/Terminated'] == 'Active':
            return row
    return None

def get_active_sensors(boat_name, csv_data):
    reader = csv.DictReader(csv_data[1:])  # Skip header
    return [sensor['Mangopare serial number'].lstrip('0') for sensor in reader if sensor['Vessel name'] == boat_name and sensor['Active/Terminated'] == 'Active']

def extract_date_from_key(object_key):
    last_date_match = re.search(r'(\d{2})(\d{2})(\d{2})', object_key)
    if last_date_match:
        return datetime.datetime(int(last_date_match.group(1)) + 2000, int(last_date_match.group(2)), int(last_date_match.group(3))).strftime('%d %b %Y')
    return None

def get_existing_html(bucket, key):
    try:
        html_obj = s3.get_object(Bucket=bucket, Key=key)
        return html_obj['Body'].read().decode('utf-8')
    except s3.exceptions.ClientError:
        return None

def update_html(existing_html, boat_name, sensor_list_str, last_date, logger):
    # Check if a row for this boat already exists
    existing_row_match = re.search(
        rf'<tr>\s*<td><span class="k1">{re.escape(boat_name)}</span></td>.*?</tr>',
        existing_html,
        flags=re.DOTALL
    )
    
    logger.info(f'Existing row match: {existing_row_match}')

    if existing_row_match:
        existing_row = existing_row_match.group(0)
        logger.info(f'Existing row content: {existing_row}')
        
        # Extract the values of the other columns using a regex pattern that matches each column separately
        columns = re.findall(r'<td><span class="k1">([^<]*)</span></td>', existing_row)
        
        logger.info(f'Extracted columns: {columns}')
        
        if len(columns) >= 6:
            du_number = columns[3]  # 4th column
            battery = columns[4]    # 5th column
            last_comm = columns[5]  # 6th column
        else:
            du_number = 'Unknown'
            battery = 'NA'
            last_comm = last_date
        
        logger.info(f'DU Number was: {du_number}')
        logger.info(f'Battery was: {battery}')
        logger.info(f'Last comm was: {last_comm}')
    else:
        du_number = 'Unknown'
        battery = 'NA'
        last_comm = last_date

    # Create the new table row for the boat with the updated last data
    new_row = f"""
    <tr>
      <td><span class="k1">{boat_name}</span></td>
      <td><span class="k1">{sensor_list_str}</span></td>
      <td><span class="k1">{last_date}</span></td>
      <td><span class="k1">{du_number}</span></td>
      <td><span class="k1">{battery}</span></td>
      <td><span class="k1">{last_comm}</span></td>
    </tr>"""
    
    logger.info(f'New row is: {new_row}')

    # Remove the existing row for this boat, if it exists
    existing_html = re.sub(
        rf'<tr>\s*<td><span class="k1">{re.escape(boat_name)}</span></td>.*?</tr>', 
        '', 
        existing_html,
        flags=re.DOTALL
    )

    # Insert the new row just after the table header row
    updated_html = re.sub(r'(<tr>\s*<th>Boat Name</th>.*?</tr>)', f'\\1\n{new_row}', existing_html, flags=re.DOTALL)

    # Update the colors based on the date
    one_month_ago = datetime.datetime.now() - datetime.timedelta(days=30)
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(days=14)
    
    updated_lines = []
    for line in updated_html.split('\n'):
        date_match = re.search(r'<td><span class="k1">(\d{1,2} \w+ \d{4})</span></td>', line)
        if date_match:
            date_str = date_match.group(1)
            date_obj = datetime.datetime.strptime(date_str, '%d %b %Y')
            if date_obj < one_month_ago:
                line = re.sub(r'class="(k1|o1)"', 'class="r1"', line)
            elif date_obj < two_weeks_ago:
                line = re.sub(r'class="(k1|r1)"', 'class="o1"', line)
            else:
                line = re.sub(r'class="(r1|o1)"', 'class="k1"', line)
        updated_lines.append(line)

    return '\n'.join(updated_lines)

def upload_html_to_s3(bucket, key, html_content):
    s3.put_object(Body=html_content.encode('utf-8'), Bucket=bucket, Key=key)

def html_web_stats(event, context):
    out_dir = "fishsoop-webstats"
    metafile_dir = "fishsoop-qc-tools"
    fishers_metafile = "Trial_fisherman_database_ausTest.csv"
    filename = "fishsoop-web-stats.html"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    sensor_number = get_sensor_number_from_event(event)
    if not sensor_number:
        raise ValueError("Sensor number not found in the event.")

    try:
        boat_csv = read_csv_from_s3(metafile_dir, fishers_metafile)
        boat_info = get_boat_info(sensor_number, boat_csv)
        
        if boat_info:
            boat_name = boat_info['Vessel name']
            active_sensors = get_active_sensors(boat_name, boat_csv)
            sensor_list_str = ', '.join(set([sensor_number] + active_sensors))

            last_date = extract_date_from_key(event['Records'][0]['s3']['object']['key'])
            if not last_date:
                raise Exception('Error extracting date from file name')

            existing_html = get_existing_html(out_dir, filename)
            if not existing_html:
                base_html = """
                <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
                <html>
                <head>
                  <!-- ... (existing styles and meta tags) ... -->
                </head>
                <body>
                    <h1 style="font-size: 24px; font-weight: bold;">FishSOOP Monitoring</h1><br><br>
                    <table>
                    <tr>
                        <th>Boat Name</th>
                        <th>Sensor SN</th>
                        <th>Last Data</th>
                        <th>DU Number</th>
                        <th>Battery (%)</th>
                        <th>Last Comm</th>
                    </tr>
                    </table>
                </body>
                </html>"""
                existing_html = base_html

            # Call the updated function to modify the HTML
            updated_html = update_html(existing_html, boat_name, sensor_list_str, last_date, logger)

            # Upload the updated HTML to S3
            upload_html_to_s3(out_dir, filename, updated_html)

            logger.info('HTML file updated')

            return {
                'statusCode': 200,
                'body': 'HTML file updated successfully'
            }

        else:
            logger.info(f"Can't find boat information for sensor number: {sensor_number}")
            return {
                'statusCode': 404,
                'body': 'Boat information not found for the sensor number'
            }

    except Exception as e:
        raise Exception(f'Error processing request: {str(e)}')

