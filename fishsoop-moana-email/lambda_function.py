import logging
import os
import boto3
from wrapper import SendDataWrapper  # Import the SendDataWrapper class from the main code file

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.info("Starting lambda")

def lambda_handler(event, context):
    try:
        logger.error("Trying lambda_handler")
        # Get the bucket and key information from the event
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']

        # Construct the full S3 object path
        qc_file_path = f"s3://{bucket_name}/{object_key}"
        logger.error(f"File is {qc_file_path}")

        # Instantiate the SendDataWrapper class and call its run() method
        senddata_wrapper = SendDataWrapper(filelist=[qc_file_path], event=event)
        logger.error(f"Started SendDataWrapper in lambda_handler")
        success_files = senddata_wrapper.run()
        logger.error(f"Ran senddata_wrapper in lambda_handler")

        logger.error("Successfully processed files: %s", success_files)
    except Exception as e:
        logger.error("Error occurred: %s", e)
        raise e
