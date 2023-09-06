import csv
import urllib.parse
import os
import tempfile
import awswrangler as wr
import logging
import re
import boto3
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def clean_and_convert_to_bigint(value):
    # Use a regular expression to extract integers from the value
    integers = re.findall(r'\d+', value)
    
    # Join the extracted integers to form a single string
    cleaned_value = ''.join(integers)
    
    return cleaned_value

def clean_and_convert_to_decimal(value):
    # Remove any non-numeric characters except for periods
    cleaned_value = re.sub(r'[^0-9.]', '', value)
    
    return cleaned_value

def process_csv(csv_file_path, columns_to_process):
    modified_rows = []

    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            modified_row = {}
            for key, value in row.items():
                if key in columns_to_process:
                    if key == 'ratings':
                        # Convert 'ratings' to decimal, handle empty and non-numeric values
                        try:
                            cleaned_value = clean_and_convert_to_decimal(value)
                            if cleaned_value:
                                modified_row[key] = Decimal(cleaned_value)
                            else:
                                # Set to None for empty values
                                modified_row[key] = None
                        except decimal.InvalidOperation:
                            # Handle invalid decimal values by setting to None
                            modified_row[key] = None
                    elif value:
                        # Clean and convert to BIGINT for other specified columns
                        try:
                            cleaned_value = clean_and_convert_to_bigint(value)
                            if cleaned_value:
                                modified_row[key] = int(cleaned_value)
                            else:
                                # Set to None for empty values
                                modified_row[key] = None
                        except ValueError:
                            # Handle non-integer values by setting to None
                            modified_row[key] = None
                    else:
                        # Set to None for empty strings
                        modified_row[key] = None
                else:
                    # Keep other columns as they are
                    modified_row[key] = value
            modified_rows.append(modified_row)

    return modified_rows

def lambda_handler(event, context):
    try:
        # Get the S3 bucket and object key from the S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        # key = event['Records'][0]['s3']['object']['key']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        # Specify the columns to process
        columns_to_process = ['discount_price', 'actual_price', 'ratings']

        # Download the CSV file to a temporary directory
        temp_dir = tempfile.mkdtemp()
        temp_csv_file = os.path.join(temp_dir, 'temp.csv')
        wr.s3.download(path=f's3://{bucket}/{key}', local_file=temp_csv_file)

        # Process the specified columns to convert 'ratings' to decimal and others to BIGINT
        modified_data = process_csv(temp_csv_file, columns_to_process)

        # Specify the path for the modified CSV file in the clean bucket # 'amazon-products-sales-analysis-clean-useast2-dev'
        clean_bucket = os.environ['clean_bucket']  
        modified_key = key.replace('.csv', '_modified.csv')

        # Write the modified data to a new CSV file
        modified_csv_file = os.path.join(temp_dir, 'modified.csv')
        with open(modified_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = modified_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='#')  # Change delimiter to "#"
            writer.writeheader()
            writer.writerows(modified_data)

        # Upload the modified CSV file to the new S3 bucket
        wr.s3.upload(local_file=modified_csv_file, path=f's3://{clean_bucket}/{modified_key}')

        # Clean up the temporary directory
        os.remove(temp_csv_file)
        os.remove(modified_csv_file)
        os.rmdir(temp_dir)

        return {
            'statusCode': 200,
            'body': 'CSV file processed and modified file stored in a different bucket.'
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise e
