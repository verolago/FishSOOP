import logging
import os
import boto3
from buildhtml import html_web_stats

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    try:
        # Print the entire event to CloudWatch Logs for inspection
        #logger.info("Event received: %s", event)
        
        # Call the html_web_stats function after QC processing
        html_web_stats(event, context)
        
    except Exception as e:
        logger.error("Error occurred: %s", e, exc_info=True)
        raise e
